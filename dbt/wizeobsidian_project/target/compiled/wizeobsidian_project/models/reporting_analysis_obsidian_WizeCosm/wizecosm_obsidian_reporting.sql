

WITH NLP AS (
    SELECT
        last_parent,
        nb_note
    FROM "wizecosm_NAS"."wizeobsidian"."note_per_last_parent"
),
FNLP AS (
    SELECT
        last_parent,
        nb_note
    FROM "wizecosm_NAS"."wizeobsidian"."filed_note_per_last_parent"
)

SELECT
    NLP.last_parent AS subject,
    NLP.nb_note AS total_note_of_subject,
    COALESCE(FNLP.nb_note, 0) AS total_field_notes_of_subject,
    (COALESCE(FNLP.nb_note, 0) * 1.00 / NLP.nb_note * 1.00) * 100 AS percent_subject_filed
FROM NLP
LEFT JOIN FNLP
    ON FNLP.last_parent = NLP.last_parent