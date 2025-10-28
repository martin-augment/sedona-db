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
use arrow_array::builder::BinaryBuilder;
use datafusion_common::error::Result;
use datafusion_expr::{
    scalar_doc_sections::DOC_SECTION_OTHER, ColumnarValue, Documentation, Volatility,
};
use geo_traits::{CoordTrait, GeometryTrait, LineStringTrait};
use sedona_expr::scalar_udf::{SedonaScalarKernel, SedonaScalarUDF};
use sedona_geometry::{
    error::SedonaGeometryError,
    wkb_factory::{write_wkb_coord, write_wkb_point_header, WKB_MIN_PROBABLE_BYTES},
};
use sedona_schema::{
    datatypes::{SedonaType, WKB_GEOMETRY},
    matchers::ArgMatcher,
};
use std::{io::Write, sync::Arc};

use crate::executor::WkbExecutor;

/// ST_StartPoint() scalar UDF
///
/// Native implementation to get the start point of a geometry
pub fn st_start_point_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new(
        "st_start_point",
        vec![Arc::new(STStartOrEndPoint::new(true))],
        Volatility::Immutable,
        Some(st_start_point_doc()),
    )
}

fn st_start_point_doc() -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Returns the start point of a LINESTRING geometry. Returns NULL if the geometry is not a LINESTRING.",
        "ST_StartPoint (geom: Geometry)",
    )
    .with_argument("geom", "geometry: Input geometry")
    .with_sql_example("SELECT ST_StartPoint(ST_GeomFromWKT('LINESTRING(0 1, 2 3, 4 5)'))")
    .build()
}

/// ST_EndPoint() scalar UDF
///
/// Native implementation to get the end point of a geometry
pub fn st_end_point_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new(
        "st_end_point",
        vec![Arc::new(STStartOrEndPoint::new(false))],
        Volatility::Immutable,
        Some(st_end_point_doc()),
    )
}

fn st_end_point_doc() -> Documentation {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Returns the end point of a LINESTRING geometry. Returns NULL if the geometry is not a LINESTRING.",
        "ST_EndPoint (geom: Geometry)",
    )
    .with_argument("geom", "geometry: Input geometry")
    .with_sql_example("SELECT ST_EndPoint(ST_GeomFromWKT('LINESTRING(0 1, 2 3, 4 5)'))")
    .build()
}

#[derive(Debug)]
struct STStartOrEndPoint {
    from_start: bool,
}

impl STStartOrEndPoint {
    fn new(from_start: bool) -> Self {
        STStartOrEndPoint { from_start }
    }
}

impl SedonaScalarKernel for STStartOrEndPoint {
    fn return_type(&self, args: &[SedonaType]) -> Result<Option<SedonaType>> {
        let matcher = ArgMatcher::new(vec![ArgMatcher::is_geometry()], WKB_GEOMETRY);

        matcher.match_args(args)
    }

    fn invoke_batch(
        &self,
        arg_types: &[SedonaType],
        args: &[ColumnarValue],
    ) -> Result<ColumnarValue> {
        let executor = WkbExecutor::new(arg_types, args);
        let mut builder = BinaryBuilder::with_capacity(
            executor.num_iterations(),
            WKB_MIN_PROBABLE_BYTES * executor.num_iterations(),
        );

        executor.execute_wkb_void(|maybe_wkb| {
            if let Some(wkb) = maybe_wkb {
                if let geo_traits::GeometryType::LineString(line_string) = wkb.as_type() {
                    let maybe_coord = if self.from_start {
                        line_string.coord(0)
                    } else {
                        line_string.coord(line_string.num_coords() - 1)
                    };

                    if let Some(coord) = maybe_coord {
                        write_wkb_start_point(&mut builder, coord).map_err(|_| {
                            sedona_internal_err!("Failed to write WKB point header")

                        })?;
                        builder.append_value([]);
                        return Ok(());
                    }
                }
            }

            builder.append_null();
            Ok(())
        })?;

        executor.finish(Arc::new(builder.finish()))
    }
}

fn write_wkb_start_point(
    buf: &mut impl Write,
    coords: impl CoordTrait<T = f64>,
) -> Result<(), SedonaGeometryError> {
    let dim = coords.dim();
    write_wkb_point_header(buf, dim)?;

    match dim.size() {
        2 => {
            let coords_tuple = coords.x_y();
            write_wkb_coord(buf, coords_tuple)
        }
        3 => {
            let coords_tuple = (coords.x(), coords.y(), coords.nth_or_panic(2));
            write_wkb_coord(buf, coords_tuple)
        }
        4 => {
            let coords_tuple = (
                coords.x(),
                coords.y(),
                coords.nth_or_panic(2),
                coords.nth_or_panic(3),
            );
            write_wkb_coord(buf, coords_tuple)
        }
        _ => Err(SedonaGeometryError::Invalid(
            "Unsupported number of dimensions".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use datafusion_expr::ScalarUDF;
    use rstest::rstest;
    use sedona_schema::datatypes::WKB_VIEW_GEOMETRY;
    use sedona_testing::{
        compare::assert_array_equal, create::create_array, testers::ScalarUdfTester,
    };

    use super::*;

    #[test]
    fn udf_metadata() {
        let st_start_point_udf: ScalarUDF = st_start_point_udf().into();
        assert_eq!(st_start_point_udf.name(), "st_start_point");
        assert!(st_start_point_udf.documentation().is_some());

        let st_end_point_udf: ScalarUDF = st_end_point_udf().into();
        assert_eq!(st_end_point_udf.name(), "st_end_point");
        assert!(st_end_point_udf.documentation().is_some());
    }

    #[rstest]
    fn udf(#[values(WKB_GEOMETRY, WKB_VIEW_GEOMETRY)] sedona_type: SedonaType) {
        let tester_start_point =
            ScalarUdfTester::new(st_start_point_udf().into(), vec![sedona_type.clone()]);
        let tester_end_point =
            ScalarUdfTester::new(st_end_point_udf().into(), vec![sedona_type.clone()]);

        let input = create_array(
            &[
                Some("LINESTRING (1 2, 3 4, 5 6)"),
                Some("LINESTRING Z (1 2 3, 3 4 5, 5 6 7)"),
                Some("LINESTRING M (1 2 3, 3 4 5, 5 6 7)"),
                Some("LINESTRING ZM (1 2 3 4, 3 4 5 6, 5 6 7 8)"),
                Some("POINT (1 2)"),
                Some("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"),
                Some("MULTILINESTRING ((1 2, 3 4), (5 6, 7 8))"),
                None,
            ],
            &sedona_type,
        );

        let expected_start_point = create_array(
            &[
                Some("POINT (1 2)"),
                Some("POINT Z (1 2 3)"),
                Some("POINT M (1 2 3)"),
                Some("POINT ZM (1 2 3 4)"),
                None,
                None,
                None,
                None,
            ],
            &WKB_GEOMETRY,
        );

        let result_start_point = tester_start_point.invoke_array(input.clone()).unwrap();
        assert_array_equal(&result_start_point, &expected_start_point);

        let expected_end_point = create_array(
            &[
                Some("POINT (5 6)"),
                Some("POINT Z (5 6 7)"),
                Some("POINT M (5 6 7)"),
                Some("POINT ZM (5 6 7 8)"),
                None,
                None,
                None,
                None,
            ],
            &WKB_GEOMETRY,
        );

        let result_end_point = tester_end_point.invoke_array(input).unwrap();
        assert_array_equal(&result_end_point, &expected_end_point);
    }
}
