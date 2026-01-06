[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "transito-bh10-boletim_ocorrencia-bronze_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"transito-bh10-boletim_ocorrencia-bronze" = let
  Origem = Lakehouse.Contents(null),
  Navegação = Origem{[workspaceId = "3ea90164-45d1-4c22-9bab-20be78397ae5"]}[Data],
  #"Navegação 1" = Navegação{[lakehouseId = "589b6a87-8f61-4c85-a0f8-f158ca5f352f"]}[Data],
  #"Navegação 2" = #"Navegação 1"{[Id = "transito-bh10-boletim_ocorrencia-bronze", ItemKind = "Table"]}[Data],
  #"Tipo de coluna alterado" = Table.TransformColumnTypes(#"Navegação 2", {{"DATA_HORA_BOLETIM", type datetime}})
in
  #"Tipo de coluna alterado";
shared #"transito-bh10-boletim_ocorrencia-bronze_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "f847acab-edab-4719-b339-63c0b52ab9c8"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "4a0d28a4-736b-4aca-ae10-a4a33d2ae65d"]}[Data],
  TableNavigation = Navigation_2{[Id = "transito-bh10-boletim_ocorrencia-teste", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
