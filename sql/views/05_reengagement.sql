-- cjc_asq_reengagement
-- Repeat account tracking - identifies accounts with multiple ASQs
-- ============================================================================

CREATE OR REPLACE VIEW home_christopher_chalcraft.cjc_views.cjc_asq_reengagement AS
WITH account_asqs AS (
  SELECT
    a.Account_Name__c AS Account_Name,
    a.AccountId__c AS Account_Id,
    a.Id AS ASQ_Id,
    a.Name AS ASQ_Number,
    a.Request_Name__c AS ASQ_Title,
    a.Status__c,
    a.Specialization__c,
    a.Support_Type__c,
    a.CreatedDate,
    a.AssignmentDate__c,
    u.Name AS Owner_Name,
    -- Time periods
    YEAR(a.CreatedDate) AS Created_Year,
    QUARTER(a.CreatedDate) AS Created_Quarter,
    CONCAT(YEAR(a.CreatedDate), '-Q', QUARTER(a.CreatedDate)) AS Created_QTR
  FROM stitch.salesforce.approvalrequest__c a
  JOIN stitch.salesforce.user u ON a.OwnerId = u.Id
  WHERE u.ManagerId = '0053f000000pKoTAAU'
),
account_summary AS (
  SELECT
    Account_Name,
    Account_Id,
    COUNT(*) AS Total_ASQs,
    COUNT(DISTINCT Owner_Name) AS Unique_SSAs,
    MIN(CreatedDate) AS First_ASQ_Date,
    MAX(CreatedDate) AS Latest_ASQ_Date,
    DATEDIFF(MAX(CreatedDate), MIN(CreatedDate)) AS Engagement_Span_Days,

    -- Current year metrics
    SUM(CASE WHEN Created_Year = YEAR(CURRENT_DATE()) THEN 1 ELSE 0 END) AS ASQs_YTD,

    -- Current quarter metrics
    SUM(CASE
      WHEN Created_Year = YEAR(CURRENT_DATE())
        AND Created_Quarter = QUARTER(CURRENT_DATE())
      THEN 1 ELSE 0
    END) AS ASQs_QTD,

    -- Specializations requested
    COLLECT_SET(Specialization__c) AS Specializations_Used,

    -- Support types used
    COLLECT_SET(Support_Type__c) AS Support_Types_Used,

    -- Status breakdown
    SUM(CASE WHEN Status__c IN ('In Progress', 'Under Review', 'On Hold') THEN 1 ELSE 0 END) AS Active_ASQs,
    SUM(CASE WHEN Status__c IN ('Completed', 'Closed') THEN 1 ELSE 0 END) AS Completed_ASQs

  FROM account_asqs
  GROUP BY Account_Name, Account_Id
)
SELECT
  *,
  -- Re-engagement classification
  CASE
    WHEN Total_ASQs >= 5 THEN 'High Engagement'
    WHEN Total_ASQs >= 2 THEN 'Repeat Customer'
    ELSE 'Single Engagement'
  END AS Engagement_Tier,

  -- Is this a repeat customer? (2+ ASQs)
  CASE WHEN Total_ASQs >= 2 THEN 1 ELSE 0 END AS Is_Repeat_Customer

FROM account_summary
ORDER BY Total_ASQs DESC, Latest_ASQ_Date DESC;
