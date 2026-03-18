-- cjc_team_summary
-- Team-level aggregations for executive dashboard
-- ============================================================================

CREATE OR REPLACE VIEW home_christopher_chalcraft.cjc_views.cjc_team_summary AS
WITH open_asqs AS (
  SELECT
    u.Name AS Owner_Name,
    a.Id,
    a.Status__c,
    a.End_Date__c,
    a.Request_Status_Notes__c,
    a.Specialization__c,
    a.Support_Type__c,
    CASE
      WHEN a.Specialization__c LIKE '%ML%' OR a.Specialization__c LIKE '%AI%' THEN 10
      WHEN a.Specialization__c LIKE '%Delta%' THEN 5
      WHEN a.Specialization__c LIKE '%SQL%' THEN 3
      WHEN a.Support_Type__c = 'Deep Dive' THEN 8
      WHEN a.Support_Type__c = 'Technical Review' THEN 3
      ELSE 5
    END AS Estimated_Days
  FROM stitch.salesforce.approvalrequest__c a
  JOIN stitch.salesforce.user u ON a.OwnerId = u.Id
  WHERE u.ManagerId = '0053f000000pKoTAAU'
    AND a.Status__c IN ('In Progress', 'Under Review', 'On Hold', 'Submitted')
),
completed_qtd AS (
  SELECT COUNT(*) AS completed_count,
         AVG(DATEDIFF(a.LastModifiedDate, a.AssignmentDate__c)) AS avg_turnaround
  FROM stitch.salesforce.approvalrequest__c a
  JOIN stitch.salesforce.user u ON a.OwnerId = u.Id
  WHERE u.ManagerId = '0053f000000pKoTAAU'
    AND a.Status__c IN ('Completed', 'Closed')
    AND QUARTER(a.LastModifiedDate) = QUARTER(CURRENT_DATE())
    AND YEAR(a.LastModifiedDate) = YEAR(CURRENT_DATE())
),
team_capacity AS (
  SELECT
    Owner_Name,
    COUNT(*) AS open_count,
    SUM(Estimated_Days) AS total_days,
    SUM(CASE WHEN End_Date__c < CURRENT_DATE() THEN 1 ELSE 0 END) AS overdue_count,
    SUM(CASE WHEN Request_Status_Notes__c IS NULL OR LENGTH(Request_Status_Notes__c) < 10 THEN 1 ELSE 0 END) AS missing_notes,
    CASE
      WHEN SUM(Estimated_Days) <= 5 THEN 'GREEN'
      WHEN SUM(Estimated_Days) <= 10 THEN 'YELLOW'
      ELSE 'RED'
    END AS capacity_status
  FROM open_asqs
  GROUP BY Owner_Name
)
SELECT
  -- Summary metrics
  (SELECT COUNT(*) FROM open_asqs) AS Total_Open_ASQs,
  (SELECT SUM(CASE WHEN End_Date__c < CURRENT_DATE() THEN 1 ELSE 0 END) FROM open_asqs) AS Overdue_ASQs,
  (SELECT SUM(CASE WHEN Request_Status_Notes__c IS NULL OR LENGTH(Request_Status_Notes__c) < 10 THEN 1 ELSE 0 END) FROM open_asqs) AS Missing_Notes_ASQs,
  (SELECT completed_count FROM completed_qtd) AS Completed_QTD,
  (SELECT ROUND(avg_turnaround, 1) FROM completed_qtd) AS Avg_Turnaround_Days,

  -- Capacity summary
  (SELECT COUNT(*) FROM team_capacity WHERE capacity_status = 'GREEN') AS Team_Members_Green,
  (SELECT COUNT(*) FROM team_capacity WHERE capacity_status = 'YELLOW') AS Team_Members_Yellow,
  (SELECT COUNT(*) FROM team_capacity WHERE capacity_status = 'RED') AS Team_Members_Red,

  -- Overall team capacity status
  CASE
    WHEN (SELECT COUNT(*) FROM team_capacity WHERE capacity_status = 'RED') > 2 THEN 'RED'
    WHEN (SELECT COUNT(*) FROM team_capacity WHERE capacity_status IN ('RED', 'YELLOW')) > 3 THEN 'YELLOW'
    ELSE 'GREEN'
  END AS Team_Capacity_Status,

  -- Distribution summaries
  (SELECT COUNT(DISTINCT Specialization__c) FROM open_asqs) AS Unique_Specializations,
  (SELECT COUNT(DISTINCT Support_Type__c) FROM open_asqs) AS Unique_Support_Types,

  -- Snapshot timestamp
  CURRENT_TIMESTAMP() AS Snapshot_Time;
