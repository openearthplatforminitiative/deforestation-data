# Deforestation API Endpoints

## Recent loss
Returns forest cover loss aggregated over the last three years for the river basin containing the given coordinate.

### Endpoint
```
GET deforestation/recent
```

### Parameters
* `lat` - latitude in decimal degrees, required
* `lon` - longitude in decimal degrees, required

### Response
```json
{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {
                "HYBAS_ID": 1081174660,
                "NEXT_DOWN": 1081177300,
                "NEXT_SINK": 1080041020,
                "MAIN_BAS": 1080041020,
                "sub_basin_area": 15.423,
                "upstream_area": 1475.4,
                "PFAF_ID": 11520560,
                "tot_treecover_loss": 1203.1,
                "rel_treecover_loss": 0.123,
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

## Loss year
Returns forest cover loss aggregated per year (from 2001 to 2022) for the river basin containing the given coordinate.

### Endpoint
```
GET deforestation/lossyear
```

### Parameters
* `lat` - latitude in decimal degrees, required
* `lon` - longitude in decimal degrees, required

### Response
```json
{
  "type": "Feature",
  "geometry": {
    "type": "Polygon",
    "coordinates": [
        [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
        ]
  },
  "properties": {
    "forest-loss": [
        {
            "year": 2001,
            "loss": 0.13
        },
        {
            "year": 2002,
            "loss": 0.08
        },
        {
            "year": 2003,
            "loss": 0.201
        },
        ...
    ]
  }
}
```
