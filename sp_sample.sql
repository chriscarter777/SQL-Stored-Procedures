USE [MyDatabase]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[GetForecastActivities]
 (
		@activityId INT = 0, 
        @status VARCHAR(50) = NULL, 
        @priority VARCHAR(50) = NULL, 
        @fromQtr INT = 1, 
        @fromYear INT = 1, 
        @throughQtr INT = 4, 
        @throughYear INT = 9999, 
		@host VARCHAR(50) = NULL, 
		@hostSite VARCHAR(50) = NULL, 
		@critical CHAR = NULL, 
		@product VARCHAR(50) = NULL, 
		@activityType VARCHAR(50) = NULL, 
		@activityLevel VARCHAR(50) = NULL,
        @cooperative VARCHAR(1) = NULL, 
		@department VARCHAR(50) = NULL, 
		@unit VARCHAR(50) = NULL, 
		@region VARCHAR(50) = NULL, 
		@leader VARCHAR(50) = NULL,
		@member VARCHAR(50) = NULL
)
AS
-- =============================================
-- Author:		Christopher Carter
-- Create date: Dec 23, XXXX
-- Description:	Retrieves a filtered list of forecast (activityId is negative, or activityDt is future) activities, adding counts of child results, discrepancies, active discrepancies, plans, active plans and attachments.
--              Member Ids, products, departments and units are provided as comma-separated lists.
-- =============================================


-- EXEC dbo.GetForecastActivities
-- EXEC dbo.GetForecastActivities @activityId = 1722
-- EXEC dbo.GetForecastActivities @status = 'Scheduled'
-- EXEC dbo.GetForecastActivities @priority = '90'
-- EXEC dbo.GetForecastActivities @fromQtr = 1, @fromYear = 2014, @throughQtr = 4, @throughYear = 2017
-- EXEC dbo.GetForecastActivities @host = 'My Host'
-- EXEC dbo.GetForecastActivities @hostSite = 'My Site'
-- EXEC dbo.GetForecastActivities @critical = 'Y'
-- EXEC dbo.GetForecastActivities @critical = 'N'
-- EXEC dbo.GetForecastActivities @product = 'My Product'
-- EXEC dbo.GetForecastActivities @activityType = 'My Type'
-- EXEC dbo.GetForecastActivities @activityLevel = 'My Level'
-- EXEC dbo.GetForecastActivities @cooperative = 'Y'
-- EXEC dbo.GetForecastActivities @cooperative = 'N'
-- EXEC dbo.GetForecastActivities @cooperative = ''
-- EXEC dbo.GetForecastActivities @department = 'Department9'
-- EXEC dbo.GetForecastActivities @unit = 'My Unit'
-- EXEC dbo.GetForecastActivities @region = 'Oregon'
-- EXEC dbo.GetForecastActivities @leader = '12345'
-- EXEC dbo.GetForecastActivities @member = '56789'


SET NOCOUNT ON
DECLARE
		@query VARCHAR(MAX),
		@whr VARCHAR(MAX),
		@fromDate DATE,
		@throughDate DATE

SET @fromDate = DATEFROMPARTS(@fromYear, (@fromQtr * 3) - 2, 1)
SET @throughDate = EOMONTH(DATEFROMPARTS(@throughYear, @throughQtr * 3, 1))

SET @query = '
	SELECT DISTINCT a.activityId,
			a.forecastQtr, 
			a.forecastYr, 
			a.activityDt, 				 
			t.scorecardName,				 
			a.scorecardRev, 
			al.activityLevel,				 
			stat.status,
			p.priority,
			r.regionName,				 
			a.ourEmail, 
			a.hostEmail, 
			a.emailLanguage, 
			a.notes, 
			
			(SELECT TOP 1 employeeId
			FROM dbo.tbl_activityParticipants
			WHERE dbo.tbl_activityParticipants.activityId = a.activityId AND dbo.tbl_activityParticipants.leader = ''Y'')
			AS leader,
			 
			STUFF((SELECT '','' + employeeId 			
			FROM dbo.tbl_activityParticipants
			WHERE dbo.tbl_activityParticipants.activityId = a.activityId AND dbo.tbl_activityParticipants.leader != ''Y''
			FOR XML PATH('''')), 1, 1, '''')
			AS members,
			
			a.lastModBy,
			a.lastModDt,
			a.siteID,
			OtherDb.dbo.tbl_sites.hostSite,
			
			CASE OtherDb.dbo.tbl_criticalEntities.inactive
				WHEN ''N'' THEN ''Y''
				ELSE ''N''
			END
			AS critical,

			CASE OtherDb.dbo.tbl_hostAddresses.country 
				WHEN ''United States'' THEN OtherDb.dbo.tbl_hostAddresses.city + CHAR(44)+ CHAR(32) + OtherDb.dbo.tbl_hostAddresses.state_province
				ELSE OtherDb.dbo.tbl_hostAddresses.city + CHAR(44)+ CHAR(32) + OtherDb.dbo.tbl_hostAddresses.country
			END							
			AS location,

			OtherDb.dbo.tbl_hosts.hostID,
			OtherDb.dbo.tbl_hosts.hostName,
			a.crossOrg,

			STUFF((SELECT '','' + prodName 			
			FROM OtherDb.dbo.tbl_product
			LEFT OUTER JOIN OtherDb.dbo.siteProduct ON OtherDb.dbo.tbl_product.prodID = OtherDb.dbo.siteProduct.prodID
			WHERE OtherDb.dbo.siteProduct.siteID = a.siteID
			FOR XML PATH('''')), 1, 1, '''')
			AS commodities,

			STUFF((SELECT '','' + deptName 			
			FROM OtherDb.dbo.tbl_departments
			LEFT OUTER JOIN OtherDb.dbo.tbl_product ON OtherDb.dbo.tbl_departments.deptID = OtherDb.dbo.tbl_product.deptID
			LEFT OUTER JOIN OtherDb.dbo.siteProduct ON OtherDb.dbo.tbl_product.prodID = OtherDb.dbo.siteProduct.prodID
			WHERE OtherDb.dbo.siteProduct.siteID = a.siteID
			FOR XML PATH('''')), 1, 1, '''')
			AS departments,

			STUFF((SELECT '','' + unitName 			
			FROM OtherDb.dbo.tbl_units
			LEFT OUTER JOIN OtherDb.dbo.tbl_product ON OtherDb.dbo.tbl_units.unitID = OtherDb.dbo.tbl_product.unitID
			LEFT OUTER JOIN OtherDb.dbo.siteProduct ON OtherDb.dbo.tbl_product.prodID = OtherDb.dbo.siteProduct.prodID
			WHERE OtherDb.dbo.siteProduct.siteID = a.siteID
			FOR XML PATH('''')), 1, 1, '''')
			AS units,

			(SELECT COUNT(DISTINCT resultID)
			FROM dbo.tbl_results
			WHERE dbo.tbl_results.activityId = a.activityId AND dbo.tbl_results.excluded != ''Y''
			GROUP BY dbo.tbl_results.activityId)
			AS resultCount,

			ISNULL(
				(SELECT MIN(rslt) 
				FROM dbo.tbl_results
				WHERE dbo.tbl_results.activityId = a.activityId AND dbo.tbl_results.excluded != ''Y'' AND dbo.tbl_results.rslt > 0
				GROUP BY dbo.tbl_results.activityId)
				, 0)
			AS keyResult,

			ISNULL(
				(SELECT COUNT(DISTINCT dbo.tbl_discrepancies.discrID)
				FROM dbo.tbl_discrepancies
				LEFT OUTER JOIN dbo.tbl_results ON dbo.tbl_discrepancies.rsltID = dbo.tbl_results.resultID
				WHERE dbo.tbl_results.activityId = a.activityId
				GROUP BY dbo.tbl_results.activityId)
				, 0)
			AS discrepancyCount,

			ISNULL(
				(SELECT COUNT(DISTINCT dbo.tbl_discrepancies.discrID)
				FROM dbo.tbl_discrepancies
				LEFT OUTER JOIN dbo.tbl_results ON dbo.tbl_discrepancies.rsltID = dbo.tbl_results.rsltID
				WHERE dbo.tbl_results.activityId = a.activityId AND dbo.tbl_discrepancies.dispID IN (1,2,3,9)
				GROUP BY dbo.tbl_results.activityId)
				, 0)
			AS activeDiscrepancyCount,

			ISNULL(
				(SELECT COUNT(DISTINCT dbo.tbl_plans.planID)
				FROM dbo.tbl_plans
				LEFT OUTER JOIN dbo.tbl_discrepancies ON dbo.tbl_plans.discrID = dbo.tbl_discrepancies.discrID
				LEFT OUTER JOIN dbo.tbl_results ON dbo.tbl_discrepancies.rsltID = dbo.tbl_results.resultID
				WHERE dbo.tbl_results.activityId = a.activityId
				GROUP BY dbo.tbl_results.activityId)
				, 0)
			AS planCount,

			ISNULL(
				(SELECT COUNT(DISTINCT dbo.tbl_plans.planID)
				FROM dbo.tbl_plans
				LEFT OUTER JOIN dbo.tbl_discrepancies ON dbo.tbl_plans.discrID = dbo.tbl_discrepancies.discrID
				LEFT OUTER JOIN dbo.tbl_results ON dbo.tbl_discrepancies.rsltID = dbo.tbl_results.resultID
				WHERE dbo.tbl_results.activityId = a.activityId AND dbo.tbl_plans.dispID IN (1,2,3,9)
				GROUP BY dbo.tbl_results.activityId)
				, 0)
			AS openPlanCount,
				 
			ISNULL(
				(SELECT COUNT(DISTINCT fileID)
				FROM dbo.tbl_attachments
				WHERE dbo.tbl_attachments.activity = a.activityId
				GROUP BY dbo.tbl_attachments.activity)
				, 0)
			AS attachmentCount


	FROM dbo.tbl_activities a
	JOIN dbo.tbl_Types t				ON t.scorecardID = a.scorecardID
    JOIN dbo.tbl_activityLevels al		ON al.activityLevelID = a.valTypeID
    JOIN dbo.tbl_status stat			ON stat.statusID = a.statusID
    LEFT OUTER JOIN dbo.tbl_priority p  ON p.priorityID = a.priorityID
    LEFT OUTER JOIN dbo.tbl_regions r	ON r.regID = a.regionID
	JOIN OtherDb.dbo.tbl_sites			ON OtherDb.dbo.tbl_sites.siteID = a.siteID
	LEFT OUTER JOIN dbo.tbl_activityParticipants 	 ON dbo.tbl_activityParticipants.activityId = a.activityId
	LEFT OUTER JOIN OtherDb.dbo.tbl_criticalEntities ON OtherDb.dbo.tbl_criticalEntities.siteID = a.siteID
	LEFT OUTER JOIN OtherDb.dbo.tbl_hostAddresses	 ON OtherDb.dbo.tbl_hostAddresses.siteID = a.siteID
	JOIN OtherDb.dbo.tbl_hosts	    	ON OtherDb.dbo.tbl_hosts.hostID = OtherDb.dbo.tbl_sites.hostID
	'

SET @whr = 'WHERE (a.scorecardRev < 0 OR a.activityDt > GETDATE()) AND a.invalid != ''Y''
			AND (
					(activityDt BETWEEN ' + CHAR(39) + CAST(@fromDate AS VARCHAR(16))+ CHAR(39) + ' AND ' + CHAR(39)  + CAST(@throughDate AS VARCHAR(16)) + CHAR(39) + ') 
					OR 
					(
						DATEFROMPARTS(forecastYr, ((forecastQtr * 3) - 2), 1) > ' + CHAR(39) + CAST(@fromDate AS VARCHAR(16))+ CHAR(39) + '
						AND	
						EOMONTH(DATEFROMPARTS(forecastYr, (forecastQtr * 3), 1)) < ' + CHAR(39)  + CAST(@throughDate AS VARCHAR(16)) + CHAR(39) + '
					)
				)'

--construct the conditional filters
IF @activityId != 0
	BEGIN
		SET @whr = @whr + ' AND a.activityId = ' + CAST(@activityId AS VARCHAR(10))
	END

	IF @status IS NOT NULL
	BEGIN
		SET @whr = @whr + ' AND status = ' + CHAR(39) + @status + CHAR(39)
	END

IF @priority IS NOT NULL
	BEGIN
		SET @whr = @whr + ' AND priority = ' + CHAR(39) + @priority + CHAR(39)
	END

IF @host IS NOT NULL
	BEGIN
	PRINT @whr
		SET @whr = @whr + ' AND hostName = ' + CHAR(39) + @host + CHAR(39)
	END

IF @hostSite IS NOT NULL
	BEGIN
		SET @whr = @whr + ' AND hostSite = ' + CHAR(39) + @hostSite + CHAR(39)
	END

IF @critical = 'Y'
	BEGIN
		SET @whr = @whr + ' AND OtherDb.dbo.tbl_criticalEntities.inactive = ''N'''
	END

IF @critical = 'N'
	BEGIN
		SET @whr = @whr + ' AND (OtherDb.dbo.tbl_criticalEntities.inactive IS NULL OR OtherDb.dbo.tbl_criticalEntities.inactive = ''Y'')'
	END

IF @activityType IS NOT NULL
	BEGIN
		SET @whr = @whr + ' AND scorecardName =  ' + CHAR(39) + @activityType + CHAR(39)
	END

IF @activityLevel IS NOT NULL
	BEGIN
		SET @whr = @whr + ' AND activityLevel = ' + CHAR(39) + @activityLevel + CHAR(39)
	END

IF @region IS NOT NULL
	BEGIN
		SET @whr = @whr + ' AND regionName = ' + CHAR(39) + @region + CHAR(39)
	END

IF @leader IS NOT NULL
	BEGIN
		PRINT @leader
		SET @whr = @whr + ' AND dbo.tbl_activityParticipants.employeeId = ' + CHAR(39) + @leader + CHAR(39)
		PRINT @whr
	END

IF @member IS NOT NULL
	BEGIN
		SET @whr = @whr + ' AND ' + Char(39) + @member + Char(39) + ' IN (SELECT employeeId 			
																			FROM dbo.tbl_activityParticipants
																			WHERE dbo.tbl_activityParticipants.activityId = a.activityId AND dbo.tbl_activityParticipants.lead != ''Y'' 
																			)' 
	END

IF @department IS NOT NULL
	BEGIN
		SET @whr = @whr + ' AND ' + Char(39) + @department + Char(39) + ' IN (SELECT deptName 			
																				FROM OtherDb.dbo.tbl_departments
																				JOIN OtherDb.dbo.tbl_product ON OtherDb.dbo.tbl_departments.deptID = OtherDb.dbo.tbl_product.deptID
																				JOIN OtherDb.dbo.siteProduct ON OtherDb.dbo.tbl_product.prodID = OtherDb.dbo.siteProduct.prodID
																				WHERE OtherDb.dbo.siteProduct.siteID = a.siteID
																				)' 
	END

IF @unit IS NOT NULL
	BEGIN
		SET @whr = @whr + ' AND  ' + Char(39) + @unit + Char(39) + ' IN (SELECT unitName 			
																			FROM OtherDb.dbo.tbl_units
																			JOIN OtherDb.dbo.tbl_product ON OtherDb.dbo.tbl_units.unitID = OtherDb.dbo.tbl_product.unitID
																			JOIN OtherDb.dbo.siteProduct ON OtherDb.dbo.tbl_product.prodID = OtherDb.dbo.siteProduct.prodID
																			WHERE OtherDb.dbo.siteProduct.siteID = a.siteID
																			)' 
	END

IF @product IS NOT NULL
	BEGIN
	PRINT @product
		SET @whr = @whr + ' AND ' + Char(39) + @product + Char(39) + ' IN (SELECT prodName 			
																			FROM OtherDb.dbo.tbl_product
																			JOIN OtherDb.dbo.siteProduct ON OtherDb.dbo.tbl_product.prodID = OtherDb.dbo.siteProduct.prodID
																			WHERE OtherDb.dbo.siteProduct.siteID = a.siteID
																			)'
	END

	IF @cooperative = 'Y'
	BEGIN
		SET @whr = @whr + ' AND crossOrg = ''Y'''
	END

IF @cooperative = 'N'
	BEGIN
		SET @whr = @whr + ' AND crossOrg = ''N'''
		PRINT @whr
	END



--put it all together and execute the query
SET @query = @query + @whr
SET @query = @query + 'ORDER BY a.activityId DESC'
EXEC(@query)

