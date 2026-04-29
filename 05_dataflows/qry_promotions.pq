// qry_promotions — Dataflow Gen2
// Destination: retail360_lh → silver_promotions → Replace mode
// Replace <workspace-id> and <lakehouse-id> with actual GUIDs

let
    Source      = Lakehouse.Contents(null),
    Workspace   = Source{[workspaceId = "<workspace-id>"]}[Data],
    LH          = Workspace{[lakehouseId = "<lakehouse-id>"]}[Data],
    Files       = LH{[Id = "Files"]}[Data],
    Raw         = Files{[Name = "raw"]}[Data],
    PromoFolder = Raw{[Name = "promotions"]}[Data],
    FileContent = PromoFolder{[Name = "promo_calendar.csv"]}[Content],
    Csv         = Csv.Document(FileContent, [Delimiter=",", Encoding=65001]),
    Promoted    = Table.PromoteHeaders(Csv, [PromoteAllScalars=true]),
    Typed       = Table.TransformColumnTypes(Promoted, {
                      {"promo_id",      Int64.Type},
                      {"promo_name",    type text},
                      {"start_date",    type date},
                      {"end_date",      type date},
                      {"category",      type text},
                      {"discount_pct",  type number},
                      {"traffic_boost", type number},
                      {"is_national",   type logical},
                      {"budget_eur",    Int64.Type}}),
    WithActive  = Table.AddColumn(Typed, "is_active", each
                      Date.From(DateTime.LocalNow()) >= [start_date] and
                      Date.From(DateTime.LocalNow()) <= [end_date],
                      type logical)
in
    WithActive
