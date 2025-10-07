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

use crate::types::GeometryTypeId;
use datafusion_common::{
    error::{DataFusionError, Result},
    exec_err,
};
use geo_traits::Dimensions;
use sedona_common::sedona_internal_err;

const SRID_FLAG_BIT: u32 = 0x20000000;

/// Fast-path WKB header parser
/// Performs operations lazily and caches them after the first computation
// pub struct WkbHeader<'a> {
pub struct WkbHeader {
    geometry_type: u32,
    // Not applicable for a point
    // number of points for a linestring
    // number of rings for a polygon
    // number of geometries for a MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION
    size: u32,
    // SRID if given buffer was EWKB. Otherwise, 0.
    srid: u32,
    // First x,y coordinates for a point. Otherwise (f64::NAN, f64::NAN) if empty
    first_xy: (f64, f64),
    // Dimensions of the geometry: xy, xyz, xym, or xyzm
    // dimensions: Dimensions,
    first_coord_dimensions: Option<Dimensions>,
    // buf: &'a [u8],
    // geometry_type_id: Option<GeometryTypeId>,
    // dimensions: Dimensions,
}

// impl<'a> WkbHeader<'a> {
impl WkbHeader {
    /// Creates a new [WkbHeader] from a buffer
    // pub fn try_new(buf: &'a [u8]) -> Result<Self> {
    pub fn try_new(buf: &[u8]) -> Result<Self> {
        if buf.len() < 5 {
            return sedona_internal_err!("Invalid WKB: buffer too small -> try_new");
        };

        let byte_order = buf[0];

        // Parse geometry type
        let geometry_type = match byte_order {
            0 => u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]),
            1 => u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]),
            other => return sedona_internal_err!("Unexpected byte order: {other}"),
        };

        let geometry_type_id = GeometryTypeId::try_from_wkb_id(geometry_type & 0x7)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        println!("top geometry_type_id: {:?}", geometry_type_id);

        let i;
        let srid;
        // if EWKB
        if geometry_type & SRID_FLAG_BIT != 0 {
            panic!("EWKB was detected");
            // srid = match byte_order {
            //     0 => u32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]),
            //     1 => u32::from_le_bytes([buf[5], buf[6], buf[7], buf[8]]),
            //     other => return sedona_internal_err!("Unexpected byte order: {other}"),
            // };
            // i = 9;
        } else {
            srid = 0;
            i = 5;
        }

        let size;
        if geometry_type_id == GeometryTypeId::Point {
            // Dummy value for a point
            size = 1;
        } else {
            size = match byte_order {
                0 => u32::from_be_bytes([buf[i + 0], buf[i + 1], buf[i + 2], buf[i + 3]]),
                1 => u32::from_le_bytes([buf[i + 0], buf[i + 1], buf[i + 2], buf[i + 3]]),
                other => return sedona_internal_err!("Unexpected byte order: {other}"),
            };
        }

        // Default values for empty geometries
        let first_x;
        let first_y;
        // TODO: rename to first_geom_dimensions
        let first_coord_dimensions: Option<Dimensions>;
        println!("top level buf len: {}", buf.len());

        let first_geom_idx = first_geom_idx(buf)?;
        if let Some(i) = first_geom_idx {
            println!("first_geom_idx: {}", i);
            first_coord_dimensions = Some(parse_dimensions(&buf[i..])?);
            println!("first_coord_dimensions: {:?}", first_coord_dimensions);
            (first_x, first_y) = first_xy(&buf[i..])?;
            println!("first_x: {}, first_y: {}", first_x, first_y);
        } else {
            first_coord_dimensions = None;
            first_x = f64::NAN;
            first_y = f64::NAN;
        }
        println!("");

        Ok(Self {
            // buf,
            // geometry_type_id: None,
            geometry_type,
            srid,
            size,
            first_xy: (first_x, first_y),
            first_coord_dimensions,
        })
    }

    /// Returns the geometry type id of the WKB by only parsing the header instead of the entire WKB
    /// 1 -> Point
    /// 2 -> LineString
    /// 3 -> Polygon
    /// 4 -> MultiPoint
    /// 5 -> MultiLineString
    /// 6 -> MultiPolygon
    /// 7 -> GeometryCollection
    ///
    /// Spec: https://libgeos.org/specifications/wkb/
    pub fn geometry_type_id(self) -> Result<GeometryTypeId> {
        // Only low 3 bits is for the base type, high bits include additional info
        let code = self.geometry_type & 0x7;

        let geometry_type_id = GeometryTypeId::try_from_wkb_id(code)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(geometry_type_id)
    }

    // Not applicable for a point
    // Number of points for a linestring
    // Number of rings for a polygon
    // Number of geometries for a MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION
    pub fn size(self) -> u32 {
        self.size
    }

    // SRID if given buffer was EWKB. Otherwise, 0.
    pub fn srid(self) -> u32 {
        self.srid
    }

    pub fn first_xy(self) -> (f64, f64) {
        self.first_xy
    }

    /// Returns the top-level dimension of the WKB
    pub fn dimensions(self) -> Result<Dimensions> {
        let dimensions = match self.geometry_type / 1000 {
            0 => Dimensions::Xy,
            1 => Dimensions::Xyz,
            2 => Dimensions::Xym,
            3 => Dimensions::Xyzm,
            _ => sedona_internal_err!("Unexpected code: {}", self.geometry_type)?,
        };
        Ok(dimensions)

        // TODO: move this to st_haszm
        // // 0000 -> xy
        // // 1000 -> xyz
        // // 2000 -> xym
        // // 3000 -> xyzm
        // let top_level_dimension = match self.geometry_type / 1000 {
        //     0 => Dimensions::Xy,
        //     1 => Dimensions::Xyz,
        //     2 => Dimensions::Xym,
        //     3 => Dimensions::Xyzm,
        //     _ => sedona_internal_err!("Unexpected code: {}", self.geometry_type)?,
        // };

        // // Infer dimension based on first coordinate dimension for cases where it differs from top-level
        // // e.g GEOMETRYCOLLECTION (POINT Z (1 2 3))
        // if let Some(first_coord_dimensions) = self.first_coord_dimensions {
        //     return Ok(first_coord_dimensions);
        // }

        // Ok(top_level_dimension)
    }

    pub fn first_coord_dimensions(&self) -> Option<Dimensions> {
        self.first_coord_dimensions
    }
}

// For MULITPOINT, MULTILINESTRING, MULTIPOLYGON, or GEOMETRYCOLLECTION, returns the index to the first nested
// non-collection geometry (POINT, LINESTRING, or POLYGON), or None if empty
// For POINT, LINESTRING, POLYGON, returns 0 as it already is a non-collection geometry
fn first_geom_idx(buf: &[u8]) -> Result<Option<usize>> {
    if buf.len() < 5 {
        return exec_err!("Invalid WKB: buffer too small -> first_geom_idx");
    }

    let byte_order = buf[0];
    let geometry_type = match byte_order {
        0 => u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]),
        1 => u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]),
        other => return sedona_internal_err!("Unexpected byte order: {other}"),
    };
    let geometry_type_id = GeometryTypeId::try_from_wkb_id(geometry_type & 0x7)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    println!("first_geom_idx: geometry_type_id: {:?}", geometry_type_id);

    match geometry_type_id {
        GeometryTypeId::Point | GeometryTypeId::LineString | GeometryTypeId::Polygon => {
            return Ok(Some(0))
        }
        GeometryTypeId::MultiPoint
        | GeometryTypeId::MultiLineString
        | GeometryTypeId::MultiPolygon
        | GeometryTypeId::GeometryCollection => {
            if buf.len() < 9 {
                return exec_err!("Invalid WKB: buffer too small");
            }
            let num_geometries = match byte_order {
                0 => u32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]),
                1 => u32::from_le_bytes([buf[5], buf[6], buf[7], buf[8]]),
                other => return sedona_internal_err!("Unexpected byte order: {other}"),
            };

            if num_geometries == 0 {
                return Ok(None);
            }

            let mut i = 9;
            if geometry_type & SRID_FLAG_BIT != 0 {
                i += 4;
                panic!("EWKB was detected");
            }

            // Recursive call to get the first geom of the first nested geometry
            // Add to current offset of i
            let off = first_geom_idx(&buf[i..]);
            if let Ok(Some(off)) = off {
                return Ok(Some(i + off));
            } else {
                return Ok(None);
            }
        }
        _ => return sedona_internal_err!("Unexpected geometry type: {geometry_type_id:?}"),
    }
}

// Given a point, linestring, or polygon, return the first xy coordinate
// If the geometry, is empty, (NaN, NaN) is returned
fn first_xy(buf: &[u8]) -> Result<(f64, f64)> {
    if buf.len() < 5 {
        return exec_err!("Invalid WKB: buffer too small -> first_xy");
    }

    let byte_order = buf[0];
    let geometry_type = match byte_order {
        0 => u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]),
        1 => u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]),
        other => return sedona_internal_err!("Unexpected byte order: {other}"),
    };

    let geometry_type_id = GeometryTypeId::try_from_wkb_id(geometry_type & 0x7)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    println!("first_xy: geometry_type_id: {:?}", geometry_type_id);

    // 1 (byte_order) + 4 (geometry_type) = 5
    let mut i = 5;

    // Skip the SRID if it's present
    if geometry_type & SRID_FLAG_BIT != 0 {
        i += 4;
        panic!("EWKB was detected");
    }

    if matches!(
        geometry_type_id,
        GeometryTypeId::LineString | GeometryTypeId::Polygon
    ) {
        if buf.len() < i + 4 {
            return exec_err!(
                "Invalid WKB: buffer too small -> first_xy3 {} is not < {}",
                buf.len(),
                i + 4
            );
        }
        let size = match byte_order {
            0 => u32::from_be_bytes([buf[i + 0], buf[i + 1], buf[i + 2], buf[i + 3]]),
            1 => u32::from_le_bytes([buf[i + 0], buf[i + 1], buf[i + 2], buf[i + 3]]),
            other => return sedona_internal_err!("Unexpected byte order: {other}"),
        };

        // (NaN, NaN) for empty geometries
        if size == 0 {
            return Ok((f64::NAN, f64::NAN));
        }
        // + 4 for size
        i += 4;
    }

    if buf.len() < i + 8 {
        return exec_err!(
            "Invalid WKB: buffer too small -> first_xy4 {} is not < {}",
            i + 8,
            buf.len()
        );
    }
    let x = parse_coord(&buf[i + 0..], byte_order)?;
    let y = parse_coord(&buf[i + 8..], byte_order)?;
    return Ok((x, y));

    // 9 + 8 (x) + 8 (y)
    // if buf.len() < 9 + 8 + 8 {
    //     return sedona_internal_err!("Invalid WKB: buffer too small ({} bytes)", buf.len());
    // }

    // let x = parse_coord(&buf[9..], byte_order)?;
    // let y = parse_coord(&buf[17..], byte_order)?;
    // return Ok((x, y));

    // 1 (byte_order) + 4 (geometry_type) + 4 (size) = 9
    // let i = 9;
    // let srid_offset = 0;

    // if matches!(
    //     geometry_type_id,
    //     GeometryTypeId::LineString | GeometryTypeId::Polygon
    // ) {
    //     // i is index of the first coordinate
    //     return Ok(parse_xy(&buf[i..], byte_order)?);
    // } else if matches!(
    //     geometry_type_id,
    //     GeometryTypeId::MultiPoint
    //         | GeometryTypeId::MultiLineString
    //         | GeometryTypeId::MultiPolygon
    //         | GeometryTypeId::GeometryCollection
    // ) {
    //     // i is the index of the first nested geometry
    //     let first_nested_geom_buf = &buf[i..];
    //     // Recursive call to get the first xy coord of the first nested geometry
    //     return Ok(first_xy(&first_nested_geom_buf, geometry_type_id)?);
    // } else {
    //     return sedona_internal_err!("Unexpected geometry type: {geometry_type_id:?}");
    // }

    // match geometry_type_id {
    //     // 1 (byte_order) + 4 (geometry_type) = 5 (no size)
    //     GeometryTypeId::Point => return Ok(parse_xy(&buf[5..], byte_order)?),
    //     // 1 (byte_order) + 4 (geometry_type) + 4 (size) = 9
    //     GeometryTypeId::LineString | GeometryTypeId::Polygon => return Ok(parse_xy(&buf[9..], byte_order)?),
    //     // 1 (byte_order) + 4 (geometry_type) + 4 (size) = 9
    //     GeometryTypeId::MultiPoint
    //     | GeometryTypeId::MultiLineString
    //     | GeometryTypeId::MultiPolygon
    //     | GeometryTypeId::GeometryCollection => {
    //         let size = match byte_order {
    //             0 => u32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]),
    //             1 => u32::from_le_bytes([buf[5], buf[6], buf[7], buf[8]]),
    //             other => return sedona_internal_err!("Unexpected byte order: {other}"),
    //         };
    //         // (NaN, NaN) for empty geometries
    //         if size == 0 {
    //             return Ok((f64::NAN, f64::NAN));
    //         } else {
    //             let first_nested_geom_buf = &buf[9..];
    //             // Recursive call to get the first xy coord of the first nested geometry
    //             return Ok(first_xy(&first_nested_geom_buf, geometry_type_id)?);
    //         }
    //     },
    //     _ => return sedona_internal_err!("Unexpected geometry type: {geometry_type_id:?}"),
    // }
}

// Given a buffer starting at the coordinate itself, parse the x and y coordinates
fn parse_coord(buf: &[u8], byte_order: u8) -> Result<f64> {
    if buf.len() < 8 {
        return sedona_internal_err!("Invalid WKB: buffer too small -> parse_coord");
    }

    let coord: f64 = match byte_order {
        0 => f64::from_be_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]),
        1 => f64::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]),
        other => return sedona_internal_err!("Unexpected byte order: {other}")?,
    };

    Ok(coord)
}

// Parses the top-level dimension of the geometry
fn parse_dimensions(buf: &[u8]) -> Result<Dimensions> {
    if buf.len() < 9 {
        return sedona_internal_err!("Invalid WKB: buffer too small -> parse_dimensions");
    }

    let byte_order = buf[0];

    let code = match byte_order {
        0 => u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]),
        1 => u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]),
        other => sedona_internal_err!("Unexpected byte order: {other}")?,
    };

    match code / 1000 {
        0 => Ok(Dimensions::Xy),
        1 => Ok(Dimensions::Xyz),
        2 => Ok(Dimensions::Xym),
        3 => Ok(Dimensions::Xyzm),
        _ => sedona_internal_err!("Unexpected code: {code:?}"),
    }

    // if buf.len() < 5 {
    //     return sedona_internal_err!("Invalid WKB: buffer too small ({} bytes)", buf.len());
    // }

    // let byte_order = buf[0];

    // let code = match byte_order {
    //     0 => u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]),
    //     1 => u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]),
    //     other => return sedona_internal_err!("Unexpected byte order: {other}"),
    // };

    // let geometry_type_id = GeometryTypeId::try_from_wkb_id(code)
    //     .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // match geometry_type_id {
    //     GeometryTypeId::Point | GeometryTypeId::LineString | GeometryTypeId::Polygon => {
    //         // 0000 -> xy or unspecified
    //         // 1000 -> xyz
    //         // 2000 -> xym
    //         // 3000 -> xyzm
    //         match code / 1000 {
    //             // If xy, it's possible we need to infer the dimension
    //             0 => return Ok(None),
    //             1 => return Ok(Some(Dimensions::Xyz)),
    //             2 => return Ok(Some(Dimensions::Xym)),
    //             3 => return Ok(Some(Dimensions::Xyzm)),
    //             _ => return sedona_internal_err!("Unexpected code: {code}"),
    //         };
    //     }
    //     GeometryTypeId::MultiPoint | GeometryTypeId::MultiLineString | GeometryTypeId::MultiPolygon | GeometryTypeId::GeometryCollection => {
    //         // If nested geometry, then recursive call
    //         let num_geometries = match byte_order {
    //             0 => u32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]),
    //             1 => u32::from_le_bytes([buf[5], buf[6], buf[7], buf[8]]),
    //             other => return sedona_internal_err!("Unexpected byte order: {other}"),
    //         };

    //         if num_geometries == 0 {
    //             return Ok(None);
    //         }
    //         // Recursive call to get the dimension of the first nested geometry
    //         return parse_dimensions(&buf[9..]);
    //     }
    //     _ => return sedona_internal_err!("Unexpected geometry type: {geometry_type_id:?}"),
    // }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use wkt::Wkt;

    fn make_wkb(wkt_value: &'static str) -> Vec<u8> {
        let geom = Wkt::<f64>::from_str(wkt_value).unwrap();
        let mut buf: Vec<u8> = vec![];
        wkb::writer::write_geometry(&mut buf, &geom, Default::default()).unwrap();
        buf
    }

    #[test]
    fn geometry_type_id() {
        let wkb = make_wkb("POINT (1 2)");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.geometry_type_id().unwrap(), GeometryTypeId::Point);

        let wkb = make_wkb("LINESTRING (1 2, 3 4)");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(
            header.geometry_type_id().unwrap(),
            GeometryTypeId::LineString
        );

        let wkb = make_wkb("POLYGON ((0 0, 0 1, 1 0, 0 0))");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.geometry_type_id().unwrap(), GeometryTypeId::Polygon);

        let wkb = make_wkb("MULTIPOINT ((1 2), (3 4))");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(
            header.geometry_type_id().unwrap(),
            GeometryTypeId::MultiPoint
        );

        let wkb = make_wkb("MULTILINESTRING ((1 2, 3 4))");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(
            header.geometry_type_id().unwrap(),
            GeometryTypeId::MultiLineString
        );

        let wkb = make_wkb("MULTIPOLYGON (((0 0, 0 1, 1 0, 0 0)))");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(
            header.geometry_type_id().unwrap(),
            GeometryTypeId::MultiPolygon
        );

        let wkb = make_wkb("GEOMETRYCOLLECTION (POINT (1 2))");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(
            header.geometry_type_id().unwrap(),
            GeometryTypeId::GeometryCollection
        );

        // Some cases with z and m dimensions
        let wkb = make_wkb("POINT Z (1 2 3)");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.geometry_type_id().unwrap(), GeometryTypeId::Point);

        let wkb = make_wkb("LINESTRING Z (1 2 3, 4 5 6)");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(
            header.geometry_type_id().unwrap(),
            GeometryTypeId::LineString
        );

        let wkb = make_wkb("POLYGON M ((0 0 0, 0 1 0, 1 0 0, 0 0 0))");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.geometry_type_id().unwrap(), GeometryTypeId::Polygon);
    }

    #[test]
    fn empty_geometry_type_id() {
        let wkb = make_wkb("POINT EMPTY");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.geometry_type_id().unwrap(), GeometryTypeId::Point);

        let wkb = make_wkb("LINESTRING EMPTY");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(
            header.geometry_type_id().unwrap(),
            GeometryTypeId::LineString
        );

        let wkb = make_wkb("POLYGON EMPTY");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.geometry_type_id().unwrap(), GeometryTypeId::Polygon);

        let wkb = make_wkb("MULTIPOINT EMPTY");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(
            header.geometry_type_id().unwrap(),
            GeometryTypeId::MultiPoint
        );

        let wkb = make_wkb("MULTILINESTRING EMPTY");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(
            header.geometry_type_id().unwrap(),
            GeometryTypeId::MultiLineString
        );

        let wkb = make_wkb("MULTIPOLYGON EMPTY");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(
            header.geometry_type_id().unwrap(),
            GeometryTypeId::MultiPolygon
        );

        let wkb = make_wkb("GEOMETRYCOLLECTION EMPTY");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(
            header.geometry_type_id().unwrap(),
            GeometryTypeId::GeometryCollection
        );

        // z, m cases
        let wkb = make_wkb("POINT Z EMPTY");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.geometry_type_id().unwrap(), GeometryTypeId::Point);

        let wkb = make_wkb("POINT M EMPTY");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.geometry_type_id().unwrap(), GeometryTypeId::Point);

        let wkb = make_wkb("LINESTRING ZM EMPTY");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(
            header.geometry_type_id().unwrap(),
            GeometryTypeId::LineString
        );
    }

    #[test]
    fn dimensions() {
        let wkb = make_wkb("POINT (1 2)");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.dimensions().unwrap(), Dimensions::Xy);

        let wkb = make_wkb("POINT Z (1 2 3)");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.dimensions().unwrap(), Dimensions::Xyz);

        let wkb = make_wkb("POINT M (1 2 3)");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.dimensions().unwrap(), Dimensions::Xym);

        let wkb = make_wkb("POINT ZM (1 2 3 4)");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.dimensions().unwrap(), Dimensions::Xyzm);
    }

    #[test]
    fn first_coord_dimensions() {
        // Top-level dimension is xy, while nested geometry is xyz
        let wkb = make_wkb("GEOMETRYCOLLECTION (POINT Z (1 2 3))");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.first_coord_dimensions().unwrap(), Dimensions::Xyz);
        assert_eq!(header.dimensions().unwrap(), Dimensions::Xy);

        let wkb = make_wkb("GEOMETRYCOLLECTION (POINT ZM (1 2 3 4))");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.first_coord_dimensions().unwrap(), Dimensions::Xyzm);

        let wkb = make_wkb("GEOMETRYCOLLECTION (POINT M (1 2 3))");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.first_coord_dimensions().unwrap(), Dimensions::Xym);

        let wkb = make_wkb("GEOMETRYCOLLECTION (POINT ZM (1 2 3 4))");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.first_coord_dimensions().unwrap(), Dimensions::Xyzm);
    }

    #[test]
    fn empty_geometry_dimensions() {
        // POINTs
        let wkb = make_wkb("POINT EMPTY");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.dimensions().unwrap(), Dimensions::Xy);

        let wkb = make_wkb("POINT Z EMPTY");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.dimensions().unwrap(), Dimensions::Xyz);

        let wkb = make_wkb("POINT M EMPTY");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.dimensions().unwrap(), Dimensions::Xym);

        let wkb = make_wkb("POINT ZM EMPTY");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.dimensions().unwrap(), Dimensions::Xyzm);

        // GEOMETRYCOLLECTIONs
        let wkb = make_wkb("GEOMETRYCOLLECTION EMPTY");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.dimensions().unwrap(), Dimensions::Xy);

        let wkb = make_wkb("GEOMETRYCOLLECTION Z EMPTY");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.dimensions().unwrap(), Dimensions::Xyz);

        let wkb = make_wkb("GEOMETRYCOLLECTION M EMPTY");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.dimensions().unwrap(), Dimensions::Xym);

        let wkb = make_wkb("GEOMETRYCOLLECTION ZM EMPTY");
        let header = WkbHeader::try_new(&wkb).unwrap();
        assert_eq!(header.dimensions().unwrap(), Dimensions::Xyzm);
    }
}
