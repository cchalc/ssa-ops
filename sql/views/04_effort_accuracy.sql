-- cjc_asq_effort_accuracy
-- Estimate vs actual effort comparison for calibration
-- ============================================================================

CREATE OR REPLACE VIEW home_christopher_chalcraft.cjc_views.cjc_asq_effort_accuracy AS
WITH effort_data AS (
  SELECT
    a.Id,
    a.Name AS ASQ_Number,
    a.Request_Name__c AS ASQ_Title,
    a.Status__c,
    a.Account_Name__c,
    a.Specialization__c,
    a.Support_Type__c,
    u.Name AS Owner_Name,

    -- Estimated effort (use stored or derive from type)
    COALESCE(
      a.Estimated_Effort_Days__c,
      CASE
        WHEN a.Specialization__c LIKE '%ML%' OR a.Specialization__c LIKE '%AI%' THEN 10
        WHEN a.Specialization__c LIKE '%Delta%' THEN 5
        WHEN a.Specialization__c LIKE '%SQL%' THEN 3
        WHEN a.Support_Type__c = 'Deep Dive' THEN 8
        WHEN a.Support_Type__c = 'Technical Review' THEN 3
        ELSE 5
      END
    ) AS Estimated_Days,

    -- Actual effort (if logged)
    a.Actual_Effort_Days__c AS Actual_Days,

    -- Derived actual from dates (fallback)
    DATEDIFF(a.LastModifiedDate, a.AssignmentDate__c) AS Days_In_Progress,

    -- Completion info
    a.LastModifiedDate AS Completion_Date,
    CONCAT(YEAR(a.LastModifiedDate), '-Q', QUARTER(a.LastModifiedDate)) AS Completion_Quarter

  FROM stitch.salesforce.approvalrequest__c a
  JOIN stitch.salesforce.user u ON a.OwnerId = u.Id
  WHERE u.ManagerId = '0053f000000pKoTAAU'
    AND a.Status__c IN ('Completed', 'Closed')
)
SELECT
  *,
  -- Use logged actual or fall back to derived
  COALESCE(Actual_Days, Days_In_Progress) AS Effective_Actual_Days,

  -- Accuracy ratio (actual / estimate)
  CASE
    WHEN Estimated_Days > 0 THEN
      ROUND(COALESCE(Actual_Days, Days_In_Progress) / Estimated_Days, 2)
    ELSE NULL
  END AS Effort_Ratio,

  -- Accuracy category
  CASE
    WHEN Estimated_Days IS NULL OR Estimated_Days = 0 THEN 'No Estimate'
    WHEN COALESCE(Actual_Days, Days_In_Progress) <= Estimated_Days * 0.8 THEN 'Under Estimate'
    WHEN COALESCE(Actual_Days, Days_In_Progress) <= Estimated_Days * 1.2 THEN 'Accurate'
    WHEN COALESCE(Actual_Days, Days_In_Progress) <= Estimated_Days * 1.5 THEN 'Slight Over'
    ELSE 'Significant Over'
  END AS Accuracy_Category,

  -- Variance (actual - estimate)
  COALESCE(Actual_Days, Days_In_Progress) - Estimated_Days AS Variance_Days

FROM effort_data
WHERE Estimated_Days > 0
ORDER BY Completion_Date DESC;
