CREATE PROCEDURE [Logging].[usp_LogPipelineExecution]
@RunId NVARCHAR (100), @PipelineName NVARCHAR (100), @ActivityName NVARCHAR (100), @TableName NVARCHAR (100), @RowsCopied INT, @Status NVARCHAR (20), @ErrorMessage NVARCHAR (MAX), @StartTime DATETIME2, @EndTime DATETIME2, @DurationSeconds INT
AS
BEGIN
    INSERT  INTO Logging.ExecutionLog (RunId, PipelineName, ActivityName, TableName, RowsCopied, Status, ErrorMessage, StartTime, EndTime, DurationSeconds)
    VALUES                           (@RunId, @PipelineName, @ActivityName, @TableName, @RowsCopied, @Status, @ErrorMessage, @StartTime, @EndTime, @DurationSeconds);
END
GO

CREATE PROCEDURE [Metadata].[usp_GetTablesToLoad]
AS
BEGIN
    SET NOCOUNT ON;
    SELECT   TableName,
             LoadType,
             TargetFolder,
             IncrementalColumn,
             LoadOrder
    FROM     Metadata.SourceTableConfig
    WHERE    Active = 1
    ORDER BY LoadOrder;
END
GO
