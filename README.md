# DAACS Reading Data Pipeline

This repository contains R scripts for preparing, quality-checking, harmonizing, and modeling DAACS reading assessment data across institutions and waves. The workflow covers four main stages: preprocessing and QA, LLM-assisted item-harmonization audit, descriptive and item-selection summaries, and IRT modeling. The reading pipeline uses a shared utility script with reusable helper functions for IDs, recoding, QA, missingness diagnostics, and item-level sample-size summaries. 

## Repository structure

### `01_preprocessing/`
Core data-preparation and QA pipeline.

Scripts in this folder:

1. `utils_read_pipeline.R`  
   Shared helper functions used across preprocessing scripts. Includes ID normalization, `global_id` creation, item-column helpers, recodes, missingness diagnostics, QA functions, and item-level sample-size summaries.

2. `read_v2_umgc1ua2-anSamp2-2022_qa_pipeline.R`  
   Cleans and standardizes the already-wide 2022 UMGC1 + UA2 reading dataset, renames legacy item IDs to final QIDs, applies the reading speed filter, creates clean institution-specific and combined wide datasets, and runs QA summaries. Output level: one row per student (`global_id`). 

3. `read_v2_ua23umgc-2022-23_qa_pipeline.R`  
   Builds clean long and wide reading datasets for UA23, UMGC, and their combined 2022–2023 file from raw institution-level, item-level, and assessment-template files. Main tasks include QID harmonization, duplicate same-completion row removal, retained-response filtering, student timing derivation, and final wide-file QA. Output levels: long files are one row per student-item response; wide files are one row per student.

4. `read_v2_umgc_ua_22_23_01_combined_qa_stack_and_missingness.R`  
   Stacks the cleaned 2022 and 2022–2023 wide files, harmonizes columns and types, and runs missingness diagnostics on the raw combined file.

5. `read_v2_umgc_ua_22_23_02_combined_qa_duplicate_diagnostics.R`  
   Identifies duplicate students and duplicate score patterns in the combined stacked reading dataset, summarizes likely duplicate pairs, and prepares auditable artifacts for patch-then-remove deduplication. In matched UMGC/UMGC1 duplicate pairs, the UMGC row is retained as the surviving row and missing selected values may be patched from the matched UMGC1 row. 

6. `read_v2_umgc_ua_22_23_03_combined_qa_finalize_and_describe.R`  
   Applies duplicate removals, replaces selected UMGC rows with patched versions, enforces one row per `global_id`, runs final missingness diagnostics, produces descriptive summaries, generates item-level sample-size tables, and saves the final cleaned combined dataset. 

### `02_harmonization_audit/`
LLM-assisted, human-reviewed item harmonization audit.

1. `read_v2_umgc_ua_22_23_02_combined_llm_harmonization.R`  
   Builds an audit workflow for uncertain item-identity matches after exact matching, fuzzy matching, and manual repair. The script creates candidate pools for review, generates prompt-ready audit tables, validates completed human decisions, and exports approved patch tables for later application in the preprocessing pipeline. This module is an auditable support layer for item-identity harmonization, not a replacement for human review. 

### `03_descriptives_and_item_selection/`
Downstream descriptive and item-selection summaries.

1. `read_v2_ua_22_23_17-18_describe.R`  
   Creates a traditional-college-age UAlbany subset (ages 17–18), runs missingness diagnostics, produces categorical and numeric descriptives, creates item-level response-count summaries, and exports summary tables for planned item selection. 

2. `read_v2_ua23umgc-2022-23_summary_table_for_item_selection.R`  
   Builds six domain-level summary tables for item selection based on response-count thresholds for IRT, multivariable DIF, and factor-level analyses. This is a downstream reporting script, not part of the core cleaning pipeline. 

### `04_modeling/`
Final analytic file creation and IRT modeling.

1. `read_v2_umgc_ua_22_23_final_dataset.R`  
   Creates the final frozen analytic dataset used for modeling. This step removes the 89 UMGC1 faculty/advisor trial administrations and recodes college to `ua` versus `umgc`, so the final analytic sample contains student records only. It also exports item-count summaries by item, domain, subgroup, and testlet. 

2. `read_v2_umgc_ua_22_23_model_comparison.R`  
   Runs the main full-bank IRT model comparison on the final frozen dataset: Rasch/1PL, unidimensional 2PL, and 2PL bifactor with one general factor plus passage/testlet-specific factors. The script exports model-fit summaries, theta correlations, difference summaries, and diagnostic plots.

3. `read_v2_umgc_ua_22_23_model_180trimming.R`  
   Uses the full-bank 2PL model to create item diagnostics, flag potential trimming candidates, build a reviewed trimmed item pool, refit the unidimensional 2PL on the trimmed bank, and compare full-versus-trimmed theta estimates. 

4. `read_v2_umgc_ua_22_23_trimmed_model_comparison.R`  
   Repeats the model comparison on the trimmed item bank using trimmed Rasch, trimmed unidimensional 2PL, and trimmed 2PL bifactor models. The script exports fit summaries, trimmed-bank theta correlations, difference summaries, and comparison plots.

Future domain-sensitivity and DIF scripts can also be added to this folder.

---

## Workflow overview

The repository is organized as a staged workflow:

1. Clean and QA the 2022 already-wide reading data (`01_preprocessing`)
2. Clean and QA the raw 2022–2023 reading data (`01_preprocessing`)
3. Stack cross-wave wide files and diagnose duplicates (`01_preprocessing`)
4. Finalize the combined cleaned dataset (`01_preprocessing`)
5. Run optional LLM-assisted harmonization audit for uncertain item matches (`02_harmonization_audit`)
6. Produce descriptive and item-selection summaries (`03_descriptives_and_item_selection`)
7. Create the final frozen analytic dataset and run IRT modeling workflows (`04_modeling`)

---

## Recommended run order

### Core preprocessing pipeline
Run these scripts in order:

1. `01_preprocessing/read_v2_umgc1ua2-anSamp2-2022_qa_pipeline.R`
2. `01_preprocessing/read_v2_ua23umgc-2022-23_qa_pipeline.R`
3. `01_preprocessing/read_v2_umgc_ua_22_23_01_combined_qa_stack_and_missingness.R`
4. `01_preprocessing/read_v2_umgc_ua_22_23_02_combined_qa_duplicate_diagnostics.R`
5. `01_preprocessing/read_v2_umgc_ua_22_23_03_combined_qa_finalize_and_describe.R`

### Optional harmonization audit
6. `02_harmonization_audit/read_v2_umgc_ua_22_23_02_combined_llm_harmonization.R`

### Downstream descriptives
7. `03_descriptives_and_item_selection/read_v2_ua_22_23_17-18_describe.R`
8. `03_descriptives_and_item_selection/read_v2_ua23umgc-2022-23_summary_table_for_item_selection.R`

### Modeling workflow
9. `04_modeling/read_v2_umgc_ua_22_23_final_dataset.R`
10. `04_modeling/read_v2_umgc_ua_22_23_model_comparison.R`
11. `04_modeling/read_v2_umgc_ua_22_23_model_180trimming.R`
12. `04_modeling/read_v2_umgc_ua_22_23_trimmed_model_comparison.R`

---

## Key variable conventions

Across scripts, the main conventions are:

- `global_id`: unique student identifier used in final outputs
- `age_d24`:
  - `TCAUS` if age < 24
  - `AUS` if age >= 24
- `ethnicity`: recoded to `White / Asian / Black / Hispanic / Other`
- `pell`: recoded to `No / Yes`
- `military`: recoded to `No / Yes`
- `transfer`: continuous transferred credits
- `readTime`: seconds

---

## Reading QID structure

For reading items, `QID` encodes both domain and passage/testlet position.

Structure:

- `Q` + three-digit global item number
- lowercase domain code
- final number indicating testlet/passage position

Domain codes:

- `s` = structure
- `in` = inference
- `id` = ideas
- `p` = purpose
- `l` = language

Examples:

- `Q001s1` = item 1, structure domain, testlet 1
- `Q014id3` = item 14, ideas domain, testlet 3
- `Q017p3` = item 17, purpose domain, testlet 3

The final number is not a difficulty code.

---

## Main output folders

Typical output folders created by the scripts include:

- `read_v2_umgc1ua2-anSamp2-2022_qa_outputs`
- `read_v2_ua23umgc-2022-23_qa_outputs`
- `read_v2_umgc_ua_22_23_combined_qa_outputs`
- `read_llm_audit_outputs`
- `read_v2_umgc_ua_22_23_final_outputs`
- `read_v2_umgc_ua_22_23_model_comparison_outputs`
- `read_v2_umgc_ua_22_23_model_180trimming_outputs`
- `read_v2_umgc_ua_22_23_trimmed_model_comparison_outputs`

---

## Notes

- The preprocessing pipeline prioritizes defensible comparability across institutions and waves over maximal retention of raw response history. 
- The LLM module is an auditable support layer for item-identity review. Final approved repairs remain human-reviewed.
- The final analytic sample excludes 89 UMGC1 faculty/advisor trial administrations, so the modeling dataset represents student records only.
- The full-bank and trimmed-bank modeling scripts are intended for overall reading-score comparisons; future domain-sensitivity and DIF analyses will extend the modeling stage. 
