-- Direct Reports Configuration
-- CJC's SSA direct reports for CAN region
-- This is a reference query - IDs are hardcoded as GTM hierarchy doesn't track SSA managers

SELECT
    name,
    user_id,
    region
FROM (
    VALUES
        ('Volodymyr Vragov', '005Vp000002lC2zIAE', 'CAN'),
        ('Allan Cao', '0058Y00000CPeiKQAT', 'CAN'),
        ('Harsha Pasala', '0058Y00000CP6yKQAT', 'CAN'),
        ('Réda Khouani', '0053f000000Wi00AAC', 'CAN'),
        ('Scott McKean', '005Vp0000016p45IAA', 'CAN'),
        ('Mathieu Pelletier', '0058Y00000CPn0bQAD', 'CAN')
) AS direct_reports(name, user_id, region)
