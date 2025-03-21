# API Reference

This document provides a comprehensive reference for all endpoints exposed by the geocube-ingester workflow server.

## Base URL

All endpoints are relative to the workflow server base URL (typically `http://localhost:8080` in development).

---

## Health Check

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Health check endpoint |

**Response:**
- Status: `200 OK`
- Body: `ingester ok`

---

## AOI Endpoints

AOI (Area of Interest) endpoints for managing ingestion areas.

### List AOIs

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/aoi/` | List all AOIs |

**Response:**
- Status: `200 OK`
- Body: JSON array of AOI objects

### Get AOI Status

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/aoi/{aoi}` | Get overview of workload for an AOI |

**Response:**
- Status: `200 OK`
- Body: Text summary with scene and tile counts by status

**Example Response:**
```
Scenes:
  new:     0
  pending: 10
  done:    36
  retry:   0
  failed:  0
  Total:   46
Tiles:
  new:     10
  pending: 21
  done:    15
  retry:   0
  failed:  0
  Total:   46

Root tiles : 46
  From: 2022-01-04
  To:   2022-10-12
```

### Create AOI

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/aoi/{aoi}` | Create a new AOI |

**Request Body:** AOI configuration

**Response:**
- Status: `201 Created` on success
- Status: `400 Bad Request` on invalid input
- Status: `500 Internal Server Error` on server error

### Delete AOI

| Method | Endpoint | Description |
|--------|----------|-------------|
| DELETE | `/aoi/{aoi}` | Delete an AOI and all its associated scenes and tiles |

**Response:**
- Status: `200 OK` on success
- Status: `404 Not Found` if AOI doesn't exist
- Status: `500 Internal Server Error` on server error

### Create Scene in AOI

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/aoi/{aoi}/scene` | Add a new scene and its tiles to the graph of dependencies |

**Request Body:** Scene configuration

**Response:**
- Status: `201 Created` on success
- Status: `400 Bad Request` on invalid input
- Status: `500 Internal Server Error` on server error

### List Scenes in AOI

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/aoi/{aoi}/scenes` | List all scenes of an AOI |
| GET | `/aoi/{aoi}/scenes/{status}` | List scenes filtered by status |

**Response:**
- Status: `200 OK`
- Body: JSON array of scene objects in GeoJSON format

### List Tiles in AOI

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/aoi/{aoi}/tiles/{status}` | List tiles of an AOI filtered by status |

**Response:**
- Status: `200 OK`
- Body: JSON array of tile objects

### List Root Tiles

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/aoi/{aoi}/roottiles` | List root tiles of an AOI |

**Response:**
- Status: `200 OK`
- Body: JSON array of root tile objects

### List Leaf Tiles

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/aoi/{aoi}/leaftiles` | List leaf tiles of an AOI |

**Response:**
- Status: `200 OK`
- Body: JSON array of leaf tile objects

### Get DOT Graph

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/aoi/{aoi}/dot` | Get DOT graph representation of the workflow |

**Response:**
- Status: `200 OK`
- Body: DOT graph text

### Retry AOI

| Method | Endpoint | Description |
|--------|----------|-------------|
| PUT | `/aoi/{aoi}/retry` | Retry all scenes and tiles with RETRY status |
| PUT | `/aoi/{aoi}/retry/force` | Force retry all scenes and tiles regardless of status |

**Response:**
- Status: `200 OK`
- Body: JSON `{"Scenes": N, "Tiles": M}` with counts of updated items
- Status: `204 No Content` if no items were updated
- Status: `404 Not Found` if AOI doesn't exist
- Status: `500 Internal Server Error` on server error

### Fail AOI

| Method | Endpoint | Description |
|--------|----------|-------------|
| PUT | `/aoi/{aoi}/fail` | Fail all scenes and tiles with RETRY status |
| PUT | `/aoi/{aoi}/fail/force` | Force fail all scenes and tiles regardless of status |

**Response:**
- Status: `200 OK`
- Body: JSON `{"Scenes": N, "Tiles": M}` with counts of updated items
- Status: `204 No Content` if no items were updated
- Status: `404 Not Found` if AOI doesn't exist
- Status: `500 Internal Server Error` on server error

---

## Scene Endpoints

Endpoints for managing individual scenes.

### Get Scene

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/scene/{scene}` | Get scene by ID |

**Response:**
- Status: `200 OK`
- Body: JSON scene object in GeoJSON format
- Status: `404 Not Found` if scene doesn't exist
- Status: `500 Internal Server Error` on server error

### Update Scene Data

| Method | Endpoint | Description |
|--------|----------|-------------|
| PUT | `/scene/{scene}/data` | Update scene data |

**Request Body:** Scene data updates

**Response:**
- Status: `200 OK` on success
- Status: `400 Bad Request` on invalid input
- Status: `404 Not Found` if scene doesn't exist
- Status: `500 Internal Server Error` on server error

### List Scene Tiles

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/scene/{scene}/tiles` | Get all tiles of a scene |

**Response:**
- Status: `200 OK`
- Body: JSON array of tile objects in GeoJSON format
- Status: `404 Not Found` if scene doesn't exist
- Status: `500 Internal Server Error` on server error

### Retry Scene

| Method | Endpoint | Description |
|--------|----------|-------------|
| PUT | `/scene/{scene}/retry` | Retry the scene (only if status is RETRY) |

**Response:**
- Status: `200 OK`
- Body: JSON scene object
- Status: `204 No Content` if scene was not in RETRY status
- Status: `404 Not Found` if scene doesn't exist
- Status: `500 Internal Server Error` on server error

### Fail Scene

| Method | Endpoint | Description |
|--------|----------|-------------|
| PUT | `/scene/{scene}/fail` | Tag scene and all its tiles as failed (only if status is RETRY, or if force is used) |

**Response:**
- Status: `200 OK`
- Body: JSON scene object
- Status: `204 No Content` if scene was not in RETRY status
- Status: `404 Not Found` if scene doesn't exist
- Status: `500 Internal Server Error` on server error

### Force Scene Status

| Method | Endpoint | Description |
|--------|----------|-------------|
| PUT | `/scene/{scene}/force/{status}` | Force set scene to specified status |

**URL Parameters:**
- `status`: The status to set (e.g., `pending`, `done`, `retry`, `failed`)

**Response:**
- Status: `200 OK`
- Body: JSON scene object
- Status: `404 Not Found` if scene doesn't exist
- Status: `500 Internal Server Error` on server error

---

## Tile Endpoints

Endpoints for managing individual tiles.

### Get Tile

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/tile/{tile}` | Get tile by ID |

**Response:**
- Status: `200 OK`
- Body: JSON tile object in GeoJSON format
- Status: `404 Not Found` if tile doesn't exist
- Status: `500 Internal Server Error` on server error

### Update Tile Data

| Method | Endpoint | Description |
|--------|----------|-------------|
| PUT | `/tile/{tile}/data` | Update tile data |

**Request Body:** Tile data updates

**Response:**
- Status: `200 OK` on success
- Status: `400 Bad Request` on invalid input
- Status: `404 Not Found` if tile doesn't exist
- Status: `500 Internal Server Error` on server error

### Retry Tile

| Method | Endpoint | Description |
|--------|----------|-------------|
| PUT | `/tile/{tile}/retry` | Retry the tile (only if status is RETRY) |

**Response:**
- Status: `200 OK`
- Body: JSON tile object
- Status: `204 No Content` if tile was not in RETRY status
- Status: `404 Not Found` if tile doesn't exist
- Status: `500 Internal Server Error` on server error

### Fail Tile

| Method | Endpoint | Description |
|--------|----------|-------------|
| PUT | `/tile/{tile}/fail` | Tag tile as failed (only if status is RETRY, or if force is used) |

**Response:**
- Status: `200 OK`
- Body: JSON tile object
- Status: `204 No Content` if tile was not in RETRY status
- Status: `404 Not Found` if tile doesn't exist
- Status: `500 Internal Server Error` on server error

### Force Tile Status

| Method | Endpoint | Description |
|--------|----------|-------------|
| PUT | `/tile/{tile}/force/{status}` | Force set tile to specified status |

**URL Parameters:**
- `status`: The status to set (e.g., `new`, `pending`, `done`, `retry`, `failed`)

**Response:**
- Status: `200 OK`
- Body: JSON tile object
- Status: `404 Not Found` if tile doesn't exist
- Status: `500 Internal Server Error` on server error

---

## Status Values

The following status values are used throughout the API:

| Status | Description |
|--------|-------------|
| `new` | Newly created, not yet processed |
| `pending` | Queued for processing |
| `done` | Processing completed successfully |
| `retry` | Processing failed, can be retried |
| `failed` | Processing failed permanently |

---

## Response Formats

### Success Responses

Most endpoints return:
- HTTP status code indicating success (200, 201, 204)
- JSON body with the requested data or operation result

### Error Responses

Error responses typically include:
- Appropriate HTTP status code (400, 404, 500)
- Error message in the response body

---

## Authentication

Currently, the API does not require authentication. In production environments, it is recommended to run the workflow server behind a reverse proxy with authentication enabled.

---

## Rate Limiting

No built-in rate limiting is implemented. The server may become unresponsive under heavy load, especially for operations that scan all tiles in an AOI.

---

## See Also

- [Monitoring Guide](./user-guide/monitoring.md) - User guide for monitoring ingestions
