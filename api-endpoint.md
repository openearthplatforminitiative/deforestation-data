# Deforestation API Endpoints

## Recent loss
Returns forest cover loss aggregated over the last three years for the river basin containing the given coordinate.

### Endpoint
```
GET deforestation/recent
```

### Parameters
* `lat`
* `lon`

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
    "forest-loss": 0.123
  }
}
```

## Loss year
Returns forest cover loss aggregated per year (from 2001 to 2022) for the river basin containing the given coordinate.

### Endpoint
```
GET deforestation/lossyear
```

### Parameters
* `lat`
* `lon`

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
