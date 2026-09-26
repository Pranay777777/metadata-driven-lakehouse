CREATE TABLE [Logging].[ExecutionLog](
	[LogID] [int] IDENTITY(1,1) NOT NULL,
	[RunId] [nvarchar](100) NULL,
	[PipelineName] [nvarchar](100) NULL,
	[ActivityName] [nvarchar](100) NULL,
	[TableName] [nvarchar](100) NULL,
	[RowsCopied] [int] NULL,
	[Status] [nvarchar](20) NULL,
	[ErrorMessage] [nvarchar](max) NULL,
	[StartTime] [datetime2](7) NULL,
	[EndTime] [datetime2](7) NULL,
	[DurationSeconds] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[LogID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
