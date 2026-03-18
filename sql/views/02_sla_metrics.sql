-- cjc_asq_sla_metrics
-- SLA tracking per ASQ - measures time to key milestones
-- ============================================================================

CREATE OR REPLACE VIEW home_christopher_chalcraft.cjc_views.cjc_asq_sla_metrics AS
WITH asq_history AS (
  -- Base ASQ data with calculated SLA metrics
  SELECT
    a.Id,
    a.Name AS ASQ_Number,
    a.Request_Name__c AS ASQ_Title,
    a.Status__c,
    a.Account_Name__c,
    a.CreatedDate,
    a.AssignmentDate__c,
    a.End_Date__c,
    a.Request_Status_Notes__c,
    u.Name AS Owner_Name,

    -- SLA 1: Approval Workflow (Submitted -> Under Review) - Target: < 2 days
    CASE
      WHEN a.AssignmentDate__c IS NOT NULL THEN
        DATEDIFF(a.AssignmentDate__c, a.CreatedDate)
      ELSE NULL
    END AS Days_To_Review,

    -- SLA 2: Assignment (Under Review -> In Progress) - Target: < 3 days
    -- Note: AssignmentDate__c marks when moved to In Progress
    CASE
      WHEN a.Status__c IN ('In Progress', 'On Hold', 'Completed', 'Closed') THEN
        DATEDIFF(a.AssignmentDate__c, a.CreatedDate)
      ELSE NULL
    END AS Days_To_Assignment,

    -- SLA 3: First Response (In Progress -> First Note) - Target: < 5 days
    -- Using notes presence as proxy for first response
    CASE
      WHEN a.Request_Status_Notes__c IS NOT NULL
        AND LENGTH(a.Request_Status_Notes__c) > 10
        AND a.AssignmentDate__c IS NOT NULL THEN
        -- Estimate: assume notes added within first week if present
        LEAST(DATEDIFF(CURRENT_DATE(), a.AssignmentDate__c), 7)
      ELSE NULL
    END AS Days_To_First_Response

  FROM stitch.salesforce.approvalrequest__c a
  JOIN stitch.salesforce.user u ON a.OwnerId = u.Id
  WHERE u.ManagerId = '0053f000000pKoTAAU'
)
SELECT
  *,
  -- SLA compliance flags
  CASE WHEN Days_To_Review IS NOT NULL AND Days_To_Review <= 2 THEN 1 ELSE 0 END AS Review_SLA_Met,
  CASE WHEN Days_To_Assignment IS NOT NULL AND Days_To_Assignment <= 5 THEN 1 ELSE 0 END AS Assignment_SLA_Met,
  CASE WHEN Days_To_First_Response IS NOT NULL AND Days_To_First_Response <= 5 THEN 1 ELSE 0 END AS Response_SLA_Met,

  -- Overall SLA status
  CASE
    WHEN Days_To_Review IS NULL THEN 'Pending Review'
    WHEN Days_To_Assignment IS NULL THEN 'Pending Assignment'
    WHEN Days_To_First_Response IS NULL THEN 'Pending Response'
    ELSE 'Active'
  END AS SLA_Stage,

  -- Week/Month for trending
  CONCAT(YEAR(CreatedDate), '-W', LPAD(WEEKOFYEAR(CreatedDate), 2, '0')) AS Created_Week,
  CONCAT(YEAR(CreatedDate), '-', LPAD(MONTH(CreatedDate), 2, '0')) AS Created_Month
FROM asq_history
ORDER BY CreatedDate DESC;
