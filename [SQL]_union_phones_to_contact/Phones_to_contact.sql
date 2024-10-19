DECLARE	@Unreachable int = 20,
--		@Declined int = 40,
		@Succeeded int = 60,
		@Finish_Wait_Period int = 3;


CREATE TABLE [dwh].[Fact_Contacts](

	[phone_1] [nvarchar](20) NULL,
	[phone_2] [nvarchar](20) NULL,

	[initial_item_id] [int] NULL,
	[main_item_id] [int] NULL,
	[last_item_id] [int] NULL,

	[cas_id] [int] NULL,
	[partner_id] [int] NULL,
	[agent_id] [int] NULL,

	[created_date] int,
	created_datetime datetime2,
	---------------------------------------------------

	first_call_start int,
	first_call_start_datetime datetime2,
	---------------------------------------------------
	finish_date int,
	finish_date_datetime datetime2,

	contact_stage_id int,
	contact_result_id int,

	is_finished bit null,
	[is_succeeded] [bit] NULL,
	[is_declined] [bit] NULL,
	[is_unreachable] [bit] NULL,


	Contact_Total_Duration int,
	First_Call_Waiting int,
	Unreachable_Calls_cnt int,
	Total_Calls_cnt int,
	First_Call_Response bit,
	First_Call_Success bit

) ON [PRIMARY]
GO




WITH	MyCalls
	AS	(
SELECT	Iif ([phone_from] < [phone_to], [phone_from], [phone_to]) AS phone1,
		Iif ([phone_from] > [phone_to], [phone_from], [phone_to]) AS phone2,
		--row_Number () OVER	(
		--					PARTITION BY Iif ([phone_from] < [phone_to], [phone_from], [phone_to]),
		--								Iif ([phone_from] > [phone_to], [phone_from], [phone_to])
		--					ORDER BY [call_start] ASC
		--					) AS Num,
		--duration,
		Count (*) OVER	(
							PARTITION BY Iif ([phone_from] < [phone_to], [phone_from], [phone_to]),
										Iif ([phone_from] > [phone_to], [phone_from], [phone_to])
							ORDER BY [call_start] ASC
							ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
							) AS Total_Calls_cnt_candidate,
		Sign (Count (Iif ([duration] > @Succeeded, 1, NULL)) OVER	(
							PARTITION BY Iif ([phone_from] < [phone_to], [phone_from], [phone_to]),
										Iif ([phone_from] > [phone_to], [phone_from], [phone_to])
							ORDER BY [call_start] ASC
							ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
							)) AS [is_succeeded_candidat],
		1 - Sign (Count (Iif ([duration] > @Unreachable, 1, NULL)) OVER	(
							PARTITION BY Iif ([phone_from] < [phone_to], [phone_from], [phone_to]),
										Iif ([phone_from] > [phone_to], [phone_from], [phone_to])
							ORDER BY [call_start] ASC
							ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
							)) AS [is_unreachable_candidate],
		Count (Iif ([duration] <= @Unreachable, 1, NULL)) OVER	(
							PARTITION BY Iif ([phone_from] < [phone_to], [phone_from], [phone_to]),
										Iif ([phone_from] > [phone_to], [phone_from], [phone_to])
							ORDER BY [call_start] ASC
							ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
							) AS Unreachable_Calls_cnt_candidate,
		Iif (DateAdd (day, @Finish_Wait_Period, DateAdd (second, duration, Last_Value ([call_start]) OVER	(
							PARTITION BY Iif ([phone_from] < [phone_to], [phone_from], [phone_to]),
										Iif ([phone_from] > [phone_to], [phone_from], [phone_to])
							ORDER BY [call_start] ASC
							ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
							))) < GetDate (), 1, 0) AS is_finished,
		DateAdd (second, Last_Value (duration) OVER	(
							PARTITION BY Iif ([phone_from] < [phone_to], [phone_from], [phone_to]),
										Iif ([phone_from] > [phone_to], [phone_from], [phone_to])
							ORDER BY [call_start] ASC
							ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
							), Last_Value ([call_start]) OVER	(
							PARTITION BY Iif ([phone_from] < [phone_to], [phone_from], [phone_to]),
										Iif ([phone_from] > [phone_to], [phone_from], [phone_to])
							ORDER BY [call_start] ASC
							ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
							)) AS finish_date_datetime_candidate
FROM	[dwh].[Fact_PhoneCall]
	),

	MyCalls2 AS
	(

SELECT *, 
		Iif ([is_finished] = 1, Total_Calls_cnt_candidate, NULL) AS Total_Calls_cnt,
		Iif ([is_finished] = 1, finish_date_datetime_candidate, NULL) AS finish_date_datetime,
		Iif ([is_succeeded_candidat] = 1 AND Total_Calls_cnt_candidate = 1 AND is_finished = 1, 1, 0) AS First_Call_Success,
		Iif (Total_Calls_cnt_candidate = 1 AND is_finished = 1, 1, 0) AS First_Call_Response,
		Iif ([is_succeeded_candidat] = 1 AND is_finished = 1, 1, 0) AS is_succeeded,
		Iif ([is_unreachable_candidate] = 1 AND is_finished = 1, 1, 0) AS is_unreachable,
		Iif ([is_succeeded_candidat] = 0 AND [is_unreachable_candidate] = 0 AND is_finished = 1, 1, 0) AS is_declined,
		Iif ([is_finished] = 1, Unreachable_Calls_cnt_candidate, NULL) AS Unreachable_Calls_cnt,
		Iif ([is_finished] = 1, 1, 2) AS contact_stage_id
		

FROM	MyCalls
),
	MyCalls3 AS (

SELECT	distinct *,
		CASE
			WHEN is_succeeded = 1 THEN 1 
			WHEN is_declined = 1 THEN 2
			WHEN is_unreachable = 1 THEN 3
		END AS contact_result_id
FROM	MyCalls2 
)

--UPDATE	dwh.Fact_Contacts
--SET		is_succeeded = (SELECT is_succeeded FROM MyCalls3 WHERE	phone1 = Fact_Contacts.phone1 ANd	phone2 = Fact_Contacts.phone2),
--		is_declined = (SELECT is_declined FROM MyCalls3 WHERE	phone1 = Fact_Contacts.phone1 ANd	phone2 = Fact_Contacts.phone2),
--		is_unreachable = (SELECT is_unreachable FROM MyCalls3 WHERE	phone1 = Fact_Contacts.phone1 ANd	phone2 = Fact_Contacts.phone2),
--		is_finished = (SELECT is_finished FROM MyCalls3 WHERE	phone1 = Fact_Contacts.phone1 ANd	phone2 = Fact_Contacts.phone2),

--		contact_stage_id = (SELECT contact_stage_id FROM MyCalls3 WHERE	phone1 = Fact_Contacts.phone1 ANd	phone2 = Fact_Contacts.phone2),
--		Unreachable_Calls_cnt = (SELECT Unreachable_Calls_cnt FROM MyCalls3 WHERE	phone1 = Fact_Contacts.phone1 ANd	phone2 = Fact_Contacts.phone2),
--		First_Call_Response = (SELECT First_Call_Response FROM MyCalls3 WHERE	phone1 = Fact_Contacts.phone1 ANd	phone2 = Fact_Contacts.phone2),
--		First_Call_Success = (SELECT First_Call_Success FROM MyCalls3 WHERE	phone1 = Fact_Contacts.phone1 ANd	phone2 = Fact_Contacts.phone2),
--		finish_date_datetime = (SELECT finish_date_datetime FROM MyCalls3 WHERE	phone1 = Fact_Contacts.phone1 ANd	phone2 = Fact_Contacts.phone2),
--		Total_Calls_cnt = (SELECT Total_Calls_cnt FROM MyCalls3 WHERE	phone1 = Fact_Contacts.phone1 ANd	phone2 = Fact_Contacts.phone2),
--		contact_result_id = (SELECT contact_result_id FROM MyCalls3 WHERE	phone1 = Fact_Contacts.phone1 ANd	phone2 = Fact_Contacts.phone2)

--ORDER BY finish_datetime_candidate DESC


INSERT
INTO	[dwh].[Fact_Contacts] (phone1, phone2,
								created_datetime,
								first_call_start_datetime)

SELECT	Iif ([phone_from] < [phone_to], [phone_from], [phone_to]),
		Iif ([phone_from] > [phone_to], [phone_from], [phone_to]),

		Min ([created]) AS created_datetime,
		Min ([call_start]) AS first_call_start_datetime



FROM	[dwh].[Fact_PhoneCall]
GROUP BY	Iif ([phone_from] < [phone_to], [phone_from], [phone_to]),
			Iif ([phone_from] > [phone_to], [phone_from], [phone_to])