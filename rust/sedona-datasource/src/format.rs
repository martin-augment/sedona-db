// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    any::Any,
    collections::HashMap,
    fmt::{Debug, Display},
    sync::Arc,
};

use arrow_array::RecordBatchReader;
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    config::ConfigOptions,
    datasource::{
        file_format::{file_compression_type::FileCompressionType, FileFormat, FileFormatFactory},
        listing::PartitionedFile,
        physical_plan::{
            FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileSinkConfig, FileSource,
        },
    },
};
use datafusion_catalog::{memory::DataSourceExec, Session};
use datafusion_common::{not_impl_err, DataFusionError, GetExt, Result, Statistics};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_physical_expr::{LexOrdering, LexRequirement, PhysicalExpr};
use datafusion_physical_plan::{
    filter_pushdown::{FilterPushdownPropagation, PushedDown},
    metrics::ExecutionPlanMetricsSet,
    ExecutionPlan,
};
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};
use sedona_common::sedona_internal_err;

#[async_trait]
pub trait SimpleFileFormat: Debug + Send + Sync {
    fn extension(&self) -> &str;
    fn with_options(&self, options: &HashMap<String, String>) -> Result<Arc<dyn SimpleFileFormat>>;
    async fn infer_schema(&self, location: &Object) -> Result<Schema>;
    async fn infer_stats(&self, _location: &Object, table_schema: &Schema) -> Result<Statistics> {
        Ok(Statistics::new_unknown(table_schema))
    }
    async fn open_reader(&self, args: &OpenReaderArgs)
        -> Result<Box<dyn RecordBatchReader + Send>>;
}

#[derive(Debug, Clone)]
pub struct OpenReaderArgs {
    pub src: Object,
    pub batch_size: Option<usize>,
    pub file_schema: Option<SchemaRef>,
    pub file_projection: Option<Vec<usize>>,
    pub filters: Option<Vec<Arc<dyn PhysicalExpr>>>,
}

#[derive(Debug, Clone)]
pub enum Object {
    ObjectStoreUrl(Arc<dyn ObjectStore>, ObjectStoreUrl),
    ObjetctStoreMeta(Arc<dyn ObjectStore>, ObjectMeta),
    String(String),
}

impl Display for Object {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Object::ObjectStoreUrl(_, object_store_url) => write!(f, "{object_store_url}"),
            Object::ObjetctStoreMeta(object_store, object_meta) => {
                // There's no great way to map an object_store to a url prefix.
                // This is a heuristic that should work for https and a local filesystem,
                // which is what we might be able to expect a non-DataFusion system like
                // GDAL to be able to translate.
                let object_store_debug = format!("{object_store:?}").to_lowercase();
                if object_store_debug.contains("http") {
                    write!(f, "https://{}", object_meta.location)
                } else if object_store_debug.contains("local") {
                    write!(f, "file://{}", object_meta.location)
                } else {
                    write!(f, "{object_store_debug}: {}", object_meta.location)
                }
            }
            Object::String(item) => write!(f, "{item}"),
        }
    }
}

#[derive(Debug)]
pub struct SedonaFormatFactory {
    spec: Arc<dyn SimpleFileFormat>,
}

impl SedonaFormatFactory {
    pub fn new(spec: Arc<dyn SimpleFileFormat>) -> Self {
        Self { spec }
    }
}

impl FileFormatFactory for SedonaFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(SedonaFormat {
            spec: self.spec.with_options(format_options)?,
        }))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(SedonaFormat {
            spec: self.spec.clone(),
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl GetExt for SedonaFormatFactory {
    fn get_ext(&self) -> String {
        self.spec.extension().to_string()
    }
}

#[derive(Debug)]
pub struct SedonaFormat {
    spec: Arc<dyn SimpleFileFormat>,
}

#[async_trait]
impl FileFormat for SedonaFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        self.spec.extension().to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        not_impl_err!("extension with compression type")
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas: Vec<_> = futures::stream::iter(objects)
            .map(|object| async move {
                let schema = self
                    .spec
                    .infer_schema(&Object::ObjetctStoreMeta(store.clone(), object.clone()))
                    .await?;
                Ok::<_, DataFusionError>((object.location.clone(), schema))
            })
            .boxed() // Workaround https://github.com/rust-lang/rust/issues/64552
            .buffered(state.config_options().execution.meta_fetch_concurrency)
            .try_collect()
            .await?;

        schemas.sort_by(|(location1, _), (location2, _)| location1.cmp(location2));

        let schemas = schemas
            .into_iter()
            .map(|(_, schema)| schema)
            .collect::<Vec<_>>();

        let schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(schema))
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        self.spec
            .infer_stats(
                &Object::ObjetctStoreMeta(store.clone(), object.clone()),
                &table_schema,
            )
            .await
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        config: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(DataSourceExec::from_data_source(config))
    }

    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("writing not yet supported for SimpleSedonaFormat")
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(SedonaFileSource::new(self.spec.clone()))
    }
}

#[derive(Debug, Clone)]
struct SedonaFileSource {
    spec: Arc<dyn SimpleFileFormat>,
    batch_size: Option<usize>,
    file_schema: Option<SchemaRef>,
    file_projection: Option<Vec<usize>>,
    filters: Option<Vec<Arc<dyn PhysicalExpr>>>,
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
}

impl SedonaFileSource {
    pub fn new(spec: Arc<dyn SimpleFileFormat>) -> Self {
        Self {
            spec,
            batch_size: None,
            file_schema: None,
            file_projection: None,
            filters: None,
            metrics: ExecutionPlanMetricsSet::default(),
            projected_statistics: None,
        }
    }
}

impl FileSource for SedonaFileSource {
    fn create_file_opener(
        &self,
        store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Arc<dyn FileOpener> {
        let args = OpenReaderArgs {
            src: Object::ObjectStoreUrl(store.clone(), base_config.object_store_url.clone()),
            batch_size: self.batch_size,
            file_schema: self.file_schema.clone(),
            file_projection: self.file_projection.clone(),
            filters: self.filters.clone(),
        };

        Arc::new(SimpleOpener {
            spec: self.spec.clone(),
            args,
            partition,
        })
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let source = Self {
            filters: Some(filters.clone()),
            ..self.clone()
        };

        Ok(FilterPushdownPropagation::with_parent_pushdown_result(vec![
            PushedDown::No;
            filters.len()
        ])
        .with_updated_node(Arc::new(source)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            batch_size: Some(batch_size),
            ..self.clone()
        })
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(Self {
            file_schema: Some(schema),
            ..self.clone()
        })
    }

    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(Self {
            file_projection: config.file_column_projection_indices(),
            ..self.clone()
        })
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        Arc::new(Self {
            projected_statistics: Some(statistics),
            ..self.clone()
        })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> Result<Statistics> {
        let statistics = &self.projected_statistics;
        Ok(statistics
            .clone()
            .expect("projected_statistics must be set"))
    }

    fn file_type(&self) -> &str {
        self.spec.extension()
    }

    // File formats implemented in this way can't be repartitioned. File formats that
    // benefit from this need their own FileFormat implementation.
    fn repartitioned(
        &self,
        _target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<LexOrdering>,
        _config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        Ok(None)
    }
}

#[derive(Debug, Clone)]
struct SimpleOpener {
    spec: Arc<dyn SimpleFileFormat>,
    args: OpenReaderArgs,
    partition: usize,
}

impl FileOpener for SimpleOpener {
    fn open(&self, _file_meta: FileMeta, _file: PartitionedFile) -> Result<FileOpenFuture> {
        if self.partition != 0 {
            return sedona_internal_err!("Expected SimpleOpener to open a single partition");
        }

        let self_clone = self.clone();
        Ok(Box::pin(async move {
            let reader = self_clone.spec.open_reader(&self_clone.args).await?;
            let stream =
                futures::stream::iter(reader.into_iter().map(|batch| batch.map_err(Into::into)));
            Ok(stream.boxed())
        }))
    }
}
