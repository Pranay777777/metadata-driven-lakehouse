CREATE TABLE [Metadata].[SourceTableConfig](
	[TableName] [nvarchar](100) NOT NULL,
	[LoadType] [nvarchar](20) NOT NULL,
	[TargetFolder] [nvarchar](100) NOT NULL,
	[Active] [bit] NOT NULL,
	[IncrementalColumn] [nvarchar](100) NULL,
	[LoadOrder] [int] NOT NULL
) ON [PRIMARY]
GO
