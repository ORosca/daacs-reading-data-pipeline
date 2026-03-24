# DAACS Reading Data Preparation and QA Pipelines

This repository contains R scripts for preparing, quality-checking, combining, and summarizing DAACS reading assessment data across multiple institutions and waves. The workflow builds clean student-level reading datasets, diagnoses missingness and duplicate-score patterns, patches selected duplicate rows before removal, produces final descriptive summaries, and generates item-selection tables for the planned reading study.

The repository includes one shared utilities file, five main pipeline scripts, one downstream item-selection script, and one focused descriptive subset script for younger UAlbany students.

## Repository structure

### Shared utilities

- `utils_read_pipeline.R`

This file stores reusable functions shared across scripts, including:
- core helpers for assertions, saving, loading, ID normalization, and `global_id` creation
- item-column helpers for finding, ordering, coercing, aligning, and counting item responses
- shared recodes for ethnicity and yes/no variables
- QID mapping helpers
- filtering helpers for minimum item counts and speedy reading responders
- missingness diagnostics
- QA helper functions
- item-level sample-size summaries

## Main pipeline scripts

### 1. `read_v2_umgc1ua2-anSamp2-2022_qa_pipeline.R`

Creates clean 2022 wide datasets for **UMGC1**, **UA2**, and their combined file from the already-wide AnSamp2 reading dataset.

**Purpose**
1. Load the already-wide 2022 UMGC1 + UA2 reading dataset
2. Rename item columns from legacy `qid_ua2` names to final `QID`s
3. Standardize demographic and metadata variables
4. Create clean UMGC1, UA2, and combined wide datasets
5. Filter speedy reading responders
6. Run item- and dataset-level QA summaries

**Unit of analysis**
- final outputs: one row per student (`global_id`)

### 2. `read_v2_ua23umgc-2022-24_qa_pipeline.R`

Builds clean long and wide datasets for **UA23**, **UMGC**, and their combined 2022–2024 file from raw institution-level, item-level, and assessment-template files.

**Purpose**
1. Load institution-level, item-level, and assessment-template files
2. Standardize institution-level demographics and identifiers
3. Map assessment-template items to global `QID`s using exact and fuzzy domain-constrained stem matching
4. Attach template rows and `QID`s to item-level responses using `question_id`
5. Remove same-completion duplicate item rows
6. Retain the first 18 cleaned responses per student
7. Derive student-level `readCompletionDate` and `readTime`
8. Exclude students with non-exactly-18 or duplicate-question retained responses
9. Exclude speedy students
10. Require all 18 retained responses to map to global `QID`s
11. Build clean long and wide reading datasets for UA23, UMGC, and combined
12. Export QA summaries and cleaned outputs

**Units of analysis**
- long files: one row per student-item response
- wide files: one row per student (`global_id`)

### 3. `read_v2_umgc_ua_22_24_01_combined_qa_stack_and_missingness.R`

Stacks the cleaned 2022 and 2022–2024 wide reading files, harmonizes columns, and runs missingness diagnostics on the combined dataset.

**Main tasks**
- load the two cleaned wide files
- add provenance via `source_file`
- harmonize types and columns
- align both files to a common master column structure
- stack them into one combined raw file
- run missingness diagnostics on the stacked data
- save the combined raw dataset and missingness outputs

### 4. `read_v2_umgc_ua_22_24_02_combined_qa_duplicate_diagnostics.R`

Detects duplicate students and duplicate score patterns in the stacked combined reading dataset and prepares deduplication artifacts for the finalization step.

**Main tasks**
- detect duplicate `global_id`s
- detect fully duplicated rows
- detect duplicate substantive rows
- detect duplicate score-only response patterns
- group rows by identical score pattern
- summarize whether duplicate-score rows are likely to represent the same person
- identify UMGC/UMGC1 duplicate pairs
- patch missing values in UMGC rows from matching UMGC1 rows
- create `rows_to_remove` and save duplicate-diagnostics outputs

### 5. `read_v2_umgc_ua_22_24_03_combined_qa_finalize_and_describe.R`

Applies duplicate removals, patches UMGC rows using non-missing values from matched UMGC1 rows, runs final missingness diagnostics, creates descriptive summaries, and saves the final cleaned combined reading dataset.

**Main tasks**
- load the stacked raw combined file
- load `rows_to_remove`
- load `umgc_rows_patched`
- replace original UMGC rows with patched versions
- remove selected UMGC1 duplicate rows
- enforce one row per `global_id`
- run final missingness diagnostics
- create categorical and numeric descriptives
- generate item-level sample-size summaries
- save the final cleaned dataset and QA outputs

## Additional analytic scripts

### 6. `read_v2_ua_22_24_17-18_describe.R`

Creates a focused descriptive subset for younger UAlbany students aged 17–18 after the final combined dataset has been created.

**Purpose**
- subset the final cleaned dataset to ages 17–18
- further subset to `ua2` and `ua23`
- run final missingness diagnostics
- create descriptive summaries
- save the younger UAlbany subset and outputs
- generate reading item-selection summary tables for that subgroup

This is a downstream descriptive script, not a core preprocessing pipeline. It depends on the final cleaned dataset produced by Script 5.

### 7. `read_summary_table_for_item_selection.R`

Creates six summary tables for item selection in the planned reading study by counting eligible items across reading domains under response-count thresholds.

**Purpose**
1. Read item-level response-count summaries from `item_sample_size_by_demo.csv`
2. Derive each item’s reading domain from the domain code embedded in `QID`
3. Build six domain tables for:
   - IRT 2PL Fit: 300 responses
   - IRT 2PL Fit: 150 responses
   - Multivariable DIF: 200 per group
   - Multivariable DIF: 50 per group
   - Factor Level: 100 per level
   - Factor Level: 40 per level
4. Save the six tables to an Excel workbook

This is a downstream summary/reporting script rather than a data-cleaning pipeline. It relies on outputs from the combined final dataset workflow.

## Recommended execution order

Run the scripts in this order:

1. `read_v2_umgc1ua2-anSamp2-2022_qa_pipeline.R`
2. `read_v2_ua23umgc-2022-24_qa_pipeline.R`
3. `read_v2_umgc_ua_22_24_01_combined_qa_stack_and_missingness.R`
4. `read_v2_umgc_ua_22_24_02_combined_qa_duplicate_diagnostics.R`
5. `read_v2_umgc_ua_22_24_03_combined_qa_finalize_and_describe.R`
6. `read_v2_ua_22_24_17-18_describe.R`
7. `read_summary_table_for_item_selection.R`

Scripts 1–5 are the main end-to-end reading preparation and QA pipeline. Scripts 6–7 are downstream analytic/reporting scripts built on the final cleaned outputs.

## Key variable conventions

Across scripts, the main variables are standardized as follows:

- `global_id`: unique student identifier
- `DAACS_ID`: normalized student ID within source data
- `age_d24`:
  - `TCAUS` if age < 24
  - `AUS` if age >= 24
- `ethnicity`:
  - `White`
  - `Asian`
  - `Black`
  - `Hispanic`
  - `Other`
- `pell`:
  - `No`
  - `Yes`
- `military`:
  - `No`
  - `Yes`
- `transfer`: continuous transferred credits
- `readTime`: seconds
- item variables: final `QID` columns ordered consistently across datasets

## QID construction

Final reading item IDs (`QID`) were created by mapping source-specific identifiers to a single common naming system.

Two mapping routes are used in the reading workflow:

- `qid_ua2 -> QID` for the already-wide 2022 files
- `question_id -> QID` for the raw item-level 2022–2024 files

For the 2022 already-wide file, legacy item column names are renamed directly to final `QID`s using the mapping file.

For the 2022–2024 raw reading files, `QID`s are attached through the assessment-template workflow:
1. assessment-template items are standardized,
2. template rows are matched to the master reading map using exact and fuzzy domain-constrained stem matching,
3. the matched `QID`s are then joined back to item-level responses using `question_id`.

The `QID` is the harmonized item identifier used across all cleaned reading datasets.

## QID construction

Final reading item IDs (`QID`) were created by mapping source-specific identifiers to a single common naming system.

Two mapping routes are used in the reading workflow:

- `qid_ua2 -> QID` for the already-wide 2022 files
- `question_id -> QID` for the raw item-level 2022–2024 files

For the 2022 already-wide file, legacy item column names are renamed directly to final `QID`s using the mapping file.

For the 2022–2024 raw reading files, `QID`s are attached through the assessment-template workflow:
1. assessment-template items are standardized,
2. template rows are matched to the master reading map using exact and fuzzy domain-constrained stem matching,
3. the matched `QID`s are then joined back to item-level responses using `question_id`.

The `QID` is the harmonized item identifier used across all cleaned reading datasets.

## Reading domains and testlet position encoded in QID

For reading items, the `QID` encodes both the reading domain and the item’s order within the testlet (reading passage).

Structure:
- `Q` + three-digit global item number
- lowercase domain code
- final number indicating the number of the testlet/passage

Domain codes:
- `s` = structure
- `in` = inference
- `id` = ideas
- `p` = purpose
- `l` = language

Examples:
- `Q001s1` = item 1, structure domain, first testlet
- `Q014id3` = item 14, ideas domain, third testlet
- `Q017p3` = item 17, purpose domain, third testlet
- `Q073id13` = item 73, ideas domain, item position 13 in the full passage order

The final number is not a difficulty code. It marks the item’s order within the reading passage/testlet sequence.

## Main outputs

Typical output folders created by the scripts include:

- `read_v2_umgc1ua2-anSamp2-2022_qa_outputs`
- `read_v2_ua23umgc-2022-24_qa_outputs`
- `read_v2_umgc_ua_22_24_combined_qa_outputs`
- `read_v2_ua_22_24_17-18_describe`

Representative final outputs include:
- cleaned wide 2022 UMGC1/UA2 reading datasets
- cleaned long and wide 2022–2024 UA23/UMGC reading datasets
- stacked combined raw file
- duplicate diagnostics tables
- `rows_to_remove`
- `umgc_rows_patched`
- final cleaned combined file `read_v2_umgc_ua_22_24`
- missingness plots and tables
- descriptive summaries
- item-level sample-size summaries
- younger UAlbany 17–18 subset outputs
- item-selection summary tables for the planned study
