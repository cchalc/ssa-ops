-- cjc_asq_uco_linkage
-- UCO (Use Case Opportunity) and consumption linkage
-- Links ASQs to UCOs and tracks associated consumption
-- ============================================================================

CREATE OR REPLACE VIEW home_christopher_chalcraft.cjc_views.cjc_asq_uco_linkage AS
WITH asq_base AS (
  SELECT
    a.Id AS ASQ_Id,
    a.Name AS ASQ_Number,
    a.Request_Name__c AS ASQ_Title,
    a.Status__c AS ASQ_Status,
    a.Account_Name__c,
    a.AccountId__c,
    a.Specialization__c,
    a.Support_Type__c,
    a.CreatedDate AS ASQ_Created,
    a.AssignmentDate__c,
    u.Name AS Owner_Name,
    -- UCO fields from ASQ (if direct link exists)
    a.Approved_Use_Case__c AS Linked_UCO_Id
  FROM stitch.salesforce.approvalrequest__c a
  JOIN stitch.salesforce.user u ON a.OwnerId = u.Id
  WHERE u.ManagerId = '0053f000000pKoTAAU'
),
uco_details AS (
  SELECT
    uc.Id AS UCO_Id,
    uc.Name AS UCO_Number,
    uc.Use_Case_Title__c AS UCO_Title,
    uc.Status__c AS UCO_Status,
    uc.Stage__c AS UCO_Stage,
    uc.Estimated_DBUs__c AS Estimated_DBUs,
    uc.Account__c AS UCO_Account_Id,
    uc.CreatedDate AS UCO_Created,
    -- From curated view for additional details
    curated.use_case_type,
    curated.primary_product,
    curated.estimated_arr_usd
  FROM stitch.salesforce.approved_usecase__c uc
  LEFT JOIN main.gtm_gold.core_usecase_curated curated
    ON uc.Id = curated.use_case_id
),
joined AS (
  SELECT
    asq.*,
    uco.UCO_Number,
    uco.UCO_Title,
    uco.UCO_Status,
    uco.UCO_Stage,
    uco.Estimated_DBUs,
    uco.use_case_type,
    uco.primary_product,
    uco.estimated_arr_usd,
    CASE WHEN uco.UCO_Id IS NOT NULL THEN 1 ELSE 0 END AS Has_UCO_Link
  FROM asq_base asq
  LEFT JOIN uco_details uco ON asq.Linked_UCO_Id = uco.UCO_Id
)
SELECT
  *,
  -- Linkage quality flag
  CASE
    WHEN Has_UCO_Link = 1 AND UCO_Status = 'Active' THEN 'Strong Link'
    WHEN Has_UCO_Link = 1 THEN 'Linked'
    ELSE 'No UCO Link'
  END AS Linkage_Status
FROM joined
ORDER BY ASQ_Created DESC;


-- Aggregated UCO linkage summary
CREATE OR REPLACE VIEW home_christopher_chalcraft.cjc_views.cjc_asq_uco_summary AS
SELECT
  Owner_Name,
  COUNT(*) AS Total_ASQs,
  SUM(Has_UCO_Link) AS ASQs_With_UCO,
  ROUND(100.0 * SUM(Has_UCO_Link) / COUNT(*), 1) AS UCO_Linkage_Rate_Pct,
  SUM(COALESCE(Estimated_DBUs, 0)) AS Total_Linked_DBUs,
  SUM(COALESCE(estimated_arr_usd, 0)) AS Total_Linked_ARR
FROM home_christopher_chalcraft.cjc_views.cjc_asq_uco_linkage
GROUP BY Owner_Name
ORDER BY Total_ASQs DESC;
