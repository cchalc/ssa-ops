-- cjc_asq_completed_metrics
-- Closed ASQ analysis with turnaround time and completion metrics
-- ============================================================================

CREATE OR REPLACE VIEW home_christopher_chalcraft.cjc_views.cjc_asq_completed_metrics AS
WITH completed_asqs AS (
  SELECT
    a.Id,
    a.Name AS ASQ_Number,
    a.Request_Name__c AS ASQ_Title,
    a.Status__c,
    a.Account_Name__c,
    a.Specialization__c,
    a.Support_Type__c,
    a.CreatedDate,
    a.AssignmentDate__c,
    a.End_Date__c AS Due_Date,
    a.LastModifiedDate AS Completion_Date,
    a.Actual_Effort_Days__c,
    a.Estimated_Effort_Days__c,
    a.Request_Status_Notes__c,
    u.Name AS Owner_Name,
    u.Email AS Owner_Email,
    -- Turnaround metrics
    DATEDIFF(a.LastModifiedDate, a.CreatedDate) AS Days_Total,
    DATEDIFF(a.LastModifiedDate, a.AssignmentDate__c) AS Days_In_Progress,
    DATEDIFF(a.AssignmentDate__c, a.CreatedDate) AS Days_To_Assign,
    -- Quarter for aggregation
    CONCAT(YEAR(a.LastModifiedDate), '-Q', QUARTER(a.LastModifiedDate)) AS Completion_Quarter,
    YEAR(a.LastModifiedDate) AS Completion_Year,
    MONTH(a.LastModifiedDate) AS Completion_Month,
    WEEKOFYEAR(a.LastModifiedDate) AS Completion_Week
  FROM stitch.salesforce.approvalrequest__c a
  JOIN stitch.salesforce.user u ON a.OwnerId = u.Id
  WHERE u.ManagerId = '0053f000000pKoTAAU'  -- CJC Team
    AND a.Status__c IN ('Completed', 'Closed')
)
SELECT
  *,
  -- On-time delivery flag
  CASE
    WHEN Due_Date IS NOT NULL AND Completion_Date <= Due_Date THEN 1
    ELSE 0
  END AS Delivered_On_Time,
  -- Closure quality flag (has notes + effort logged)
  CASE
    WHEN Request_Status_Notes__c IS NOT NULL
      AND LENGTH(Request_Status_Notes__c) > 50
      AND Actual_Effort_Days__c IS NOT NULL
    THEN 1
    ELSE 0
  END AS Quality_Closure
FROM completed_asqs
ORDER BY Completion_Date DESC;
