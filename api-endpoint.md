# Deforestation API Endpoints

## Tree cover loss
Returns tree cover loss aggregated over the last three years for the river basin containing the given coordinate.

### Endpoint
```
GET deforestation/lossyear
```

### Parameters
* `lat` - latitude in decimal degrees, required
* `lon` - longitude in decimal degrees, required
* `daterange` - TODO: determine date range specification

### Response
```json
{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {
                "id": 1081174660,
                "subbasin_area": 15.423,
                "upstream_area": 1475.4,
                "downstream_id": 1081174490,
                "total_treecover_loss": 1203.1,
                "relative_treecover_loss": 0.123,
                "start_year": 2020,
                "end_year": 2022,
                "lossyear": [
                    {
                        "year": 2001,
                        "total": 0.13,
                        "relative": 0.13
                    },
                    {
                        "year": 2002,
                        "total": 142.1,
                        "relative": 0.13
                    },
                    {
                        "year": 2003,
                        "total": 0.0,
                        "relative": 0.0
                    }
                ]
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [100.0, 0.0],
                        [101.0, 0.0],
                        [101.0, 1.0],
                        [100.0, 1.0],
                        [100.0, 0.0]
                    ]
                ]
            }
        }
    ]
}
```