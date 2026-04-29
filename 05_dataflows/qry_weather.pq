// qry_weather — Dataflow Gen2
// Destination: retail360_lh → silver_weather → Append mode
// Replace <workspace-id> and <lakehouse-id> with actual GUIDs

let
    Source        = Lakehouse.Contents(null),
    Workspace     = Source{[workspaceId = "<workspace-id>"]}[Data],
    LH            = Workspace{[lakehouseId = "<lakehouse-id>"]}[Data],
    Files         = LH{[Id = "Files"]}[Data],
    RawFolder     = Files{[Name = "raw"]}[Data],
    WeatherFolder = RawFolder{[Name = "weather"]}[Data],
    JsonFiles     = Table.SelectRows(WeatherFolder, each Text.EndsWith([Name], ".json")),
    Parsed        = Table.AddColumn(JsonFiles, "Json", each Json.Document([Content])),
    Expanded      = Table.ExpandListColumn(Parsed, "Json"),
    Records       = Table.ExpandRecordColumn(Expanded, "Json",
                        {"date","city","region","temp_max_c","temp_min_c",
                         "precipitation_mm","wind_kmh","is_sunny","weather_label"}),
    Selected      = Table.SelectColumns(Records,
                        {"date","city","region","temp_max_c","temp_min_c",
                         "precipitation_mm","wind_kmh","is_sunny","weather_label"}),
    Renamed       = Table.RenameColumns(Selected, {
                        {"temp_max_c",       "max_temperature_c"},
                        {"temp_min_c",       "min_temperature_c"},
                        {"precipitation_mm", "rainfall_mm"},
                        {"wind_kmh",         "wind_speed_kmh"},
                        {"weather_label",    "weather_condition"}}),
    Typed         = Table.TransformColumnTypes(Renamed, {
                        {"date",              type date},
                        {"city",              type text},
                        {"region",            type text},
                        {"max_temperature_c", type number},
                        {"min_temperature_c", type number},
                        {"rainfall_mm",       type number},
                        {"wind_speed_kmh",    type number},
                        {"is_sunny",          type logical},
                        {"weather_condition", type text}})
in
    Typed
