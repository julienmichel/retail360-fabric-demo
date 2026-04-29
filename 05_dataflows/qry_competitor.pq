// qry_competitor — Dataflow Gen2
// Destination: retail360_lh → silver_competitor_prices → Append mode
// Replace <workspace-id> and <lakehouse-id> with actual GUIDs

let
    Source      = Lakehouse.Contents(null),
    Workspace   = Source{[workspaceId = "<workspace-id>"]}[Data],
    LH          = Workspace{[lakehouseId = "<lakehouse-id>"]}[Data],
    Files       = LH{[Id = "Files"]}[Data],
    Raw         = Files{[Name = "raw"]}[Data],
    CompFolder  = Raw{[Name = "competitor_prices"]}[Data],
    CsvFiles    = Table.SelectRows(CompFolder, each Text.EndsWith([Name], ".csv")),
    Parsed      = Table.AddColumn(CsvFiles, "Csv", each
                      Table.PromoteHeaders(
                          Csv.Document([Content], [Delimiter=",",
                              Encoding=65001, QuoteStyle=QuoteStyle.None]),
                          [PromoteAllScalars=true])),
    Combined    = Table.Combine(Parsed[Csv]),
    Cleaned     = Table.SelectRows(Combined,
                      each [product_id] <> "product_id"
                        and [product_id] <> null
                        and [product_id] <> ""),
    Typed       = Table.TransformColumnTypes(Cleaned, {
                      {"snapshot_month",   type text},
                      {"product_id",       Int64.Type},
                      {"product_name",     type text},
                      {"category",         type text},
                      {"competitor",       type text},
                      {"competitor_price", type number},
                      {"our_price",        type number},
                      {"price_gap_pct",    type number},
                      {"is_cheaper",       type logical},
                      {"source",           type text}})
in
    Typed
