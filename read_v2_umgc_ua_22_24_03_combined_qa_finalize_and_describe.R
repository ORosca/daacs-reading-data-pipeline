# read_v2_umgc_ua_22_24_03_combined_qa_finalize_and_describe.R
# ============================================================
# Apply duplicate removals, patch UMGC rows with non-missing
# values from matching UMGC1 rows, run final missingness
# diagnostics, create descriptive summaries, item-level
# sample-size tables, and save the final cleaned dataset.
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(readr)
  library(stringr)
  library(tibble)
  library(purrr)
  library(ggplot2)
})

project_dir <- "C:/Users/orosc/OneDrive/Read"
source(file.path(project_dir, "utils_read_pipeline.R"))
output_dir <- file.path(project_dir, "read_v2_umgc_ua_22_24_combined_qa_outputs")

input_file <- file.path(output_dir, "read_v2_umgc_ua_2waves4sets_raw.rds")
rows_to_remove_file <- file.path(output_dir, "rows_to_remove.rds")
umgc_rows_patched_file <- file.path(output_dir, "umgc_rows_patched.rds")

stopifnot(file.exists(input_file))
stopifnot(file.exists(rows_to_remove_file))
stopifnot(file.exists(umgc_rows_patched_file))

# ------------------------------------------------------------
# Section 1. Load files and finalize cleaned dataset
# ------------------------------------------------------------

read_v2_umgc_ua_2waves4sets_raw <- readRDS(input_file)
rows_to_remove <- readRDS(rows_to_remove_file)
umgc_rows_patched <- readRDS(umgc_rows_patched_file)

assert_has_cols(
  read_v2_umgc_ua_2waves4sets_raw,
  c("global_id"),
  "read_v2_umgc_ua_2waves4sets_raw"
)

assert_has_cols(rows_to_remove, c("global_id"), "rows_to_remove")
assert_has_cols(umgc_rows_patched, c("global_id"), "umgc_rows_patched")

# These are the original UMGC rows that must be replaced by patched versions
umgc_ids_to_replace <- umgc_rows_patched %>%
  distinct(global_id)

# Final cleaned dataset:
# 1) remove original UMGC rows that will be replaced
# 2) remove UMGC1 rows marked for deletion
# 3) append patched UMGC rows
# 4) enforce one row per global_id
read_v2_umgc_ua_22_24 <- read_v2_umgc_ua_2waves4sets_raw %>%
  anti_join(umgc_ids_to_replace, by = "global_id") %>%
  anti_join(rows_to_remove, by = "global_id") %>%
  bind_rows(umgc_rows_patched) %>%
  distinct(global_id, .keep_all = TRUE)

# Quick QA checks
stopifnot(sum(duplicated(read_v2_umgc_ua_22_24$global_id)) == 0)

n_input <- nrow(read_v2_umgc_ua_2waves4sets_raw)
n_removed_umgc1 <- nrow(rows_to_remove)
n_replaced_umgc <- nrow(umgc_ids_to_replace)
n_final <- nrow(read_v2_umgc_ua_22_24)

cat("\nFinalize step summary:\n")
cat("Input raw rows:             ", n_input, "\n")
cat("UMGC1 rows removed:         ", n_removed_umgc1, "\n")
cat("Original UMGC rows replaced:", n_replaced_umgc, "\n")
cat("Patched UMGC rows added:    ", nrow(umgc_rows_patched), "\n")
cat("Final rows:                 ", n_final, "\n")

miss_nodup <- run_missingness_diagnostics(
  df = read_v2_umgc_ua_22_24,
  stage_name = "after_removing_duplicates_and_patching",
  save_plots = TRUE,
  output_dir = output_dir
)

# ------------------------------------------------------------
# Section 2. Descriptive statistics
# ------------------------------------------------------------

read_v2_umgc_ua_22_24 <- read_v2_umgc_ua_22_24 %>%
  mutate(
    college   = as.factor(college),
    wave      = as.factor(wave),
    age_d24   = as.factor(age_d24),
    gender    = as.factor(gender),
    ethnicity = as.factor(ethnicity),
    military  = as.factor(military),
    pell      = as.factor(pell),
    age       = suppressWarnings(as.numeric(age)),
    transfer  = suppressWarnings(as.numeric(transfer)),
    readTime  = suppressWarnings(as.numeric(readTime))
  )

cat_vars <- c("college", "wave", "age_d24", "gender", "ethnicity", "military", "pell")

cat_descriptives <- lapply(cat_vars, function(v) {
  read_v2_umgc_ua_22_24 %>%
    count(.data[[v]], .drop = FALSE) %>%
    mutate(
      variable = v,
      percent = 100 * n / sum(n)
    ) %>%
    rename(category = 1) %>%
    select(variable, category, n, percent)
})

cat_descriptives_df <- bind_rows(cat_descriptives)

num_descriptives <- read_v2_umgc_ua_22_24 %>%
  summarise(
    age_n = sum(!is.na(age)),
    age_mean = mean(age, na.rm = TRUE),
    age_sd = sd(age, na.rm = TRUE),
    age_min = min(age, na.rm = TRUE),
    age_q1 = as.numeric(quantile(age, 0.25, na.rm = TRUE)),
    age_median = median(age, na.rm = TRUE),
    age_q3 = as.numeric(quantile(age, 0.75, na.rm = TRUE)),
    age_max = max(age, na.rm = TRUE),
    
    transfer_n = sum(!is.na(transfer)),
    transfer_mean = mean(transfer, na.rm = TRUE),
    transfer_sd = sd(transfer, na.rm = TRUE),
    transfer_min = min(transfer, na.rm = TRUE),
    transfer_q1 = as.numeric(quantile(transfer, 0.25, na.rm = TRUE)),
    transfer_median = median(transfer, na.rm = TRUE),
    transfer_q3 = as.numeric(quantile(transfer, 0.75, na.rm = TRUE)),
    transfer_max = max(transfer, na.rm = TRUE),
    
    readTime_n = sum(!is.na(readTime)),
    readTime_mean = mean(readTime, na.rm = TRUE),
    readTime_sd = sd(readTime, na.rm = TRUE),
    readTime_min = min(readTime, na.rm = TRUE),
    readTime_q1 = as.numeric(quantile(readTime, 0.25, na.rm = TRUE)),
    readTime_median = median(readTime, na.rm = TRUE),
    readTime_q3 = as.numeric(quantile(readTime, 0.75, na.rm = TRUE)),
    readTime_max = max(readTime, na.rm = TRUE)
  )

p_age_hist <- ggplot(read_v2_umgc_ua_22_24, aes(x = age)) +
  geom_histogram(bins = 30) +
  labs(title = "Age Distribution", x = "Age", y = "Count") +
  theme_minimal()

p_transfer_hist <- ggplot(read_v2_umgc_ua_22_24, aes(x = transfer)) +
  geom_histogram(bins = 30) +
  labs(title = "Transferred Credits Distribution", x = "Transferred Credits", y = "Count") +
  theme_minimal()

p_readTime_hist <- ggplot(read_v2_umgc_ua_22_24, aes(x = readTime)) +
  geom_histogram(bins = 30) +
  labs(title = "Read Time Distribution", x = "Read Time", y = "Count") +
  theme_minimal()

p_age_box <- ggplot(read_v2_umgc_ua_22_24, aes(y = age)) +
  geom_boxplot() +
  labs(title = "Boxplot of Age", y = "Age") +
  theme_minimal()

p_transfer_box <- ggplot(read_v2_umgc_ua_22_24, aes(y = transfer)) +
  geom_boxplot() +
  labs(title = "Boxplot of Transferred Credits", y = "Transferred Credits") +
  theme_minimal()

p_readTime_box <- ggplot(read_v2_umgc_ua_22_24, aes(y = readTime)) +
  geom_boxplot() +
  labs(title = "Boxplot of Read Time", y = "Read Time") +
  theme_minimal()

# ------------------------------------------------------------
# Section 3. Item-level sample-size summary and QA
# ------------------------------------------------------------

sample_size_report <- make_item_sample_size_report(read_v2_umgc_ua_22_24)

qa_summary <- list(
  n_input_raw = n_input,
  n_removed_duplicate_umgc1_rows = n_removed_umgc1,
  n_replaced_umgc_rows = n_replaced_umgc,
  n_patched_umgc_rows_added = nrow(umgc_rows_patched),
  n_final = nrow(read_v2_umgc_ua_22_24),
  item_sample_size_overall_n = nrow(sample_size_report$overall_counts),
  item_sample_size_by_demo_n = nrow(sample_size_report$by_demo_counts)
)

# ------------------------------------------------------------
# Reading items: one-line quartile summary of response counts
# and save to a .txt file
# ------------------------------------------------------------

# Identify item columns
read_item_cols <- grep("^Q\\d+", names(read_v2_umgc_ua_22_24), value = TRUE)

# Count non-missing responses per item
read_item_counts <- sapply(read_v2_umgc_ua_22_24[read_item_cols], function(x) sum(!is.na(x)))

# Quartiles and range
q <- quantile(read_item_counts, probs = c(0, 0.25, 0.50, 0.75, 1.00), na.rm = TRUE)

# Create one summary line
summary_line <- paste0(
  "Response counts per reading item: ",
  "Min = ", unname(q[1]),
  ", Q1 = ", unname(q[2]),
  ", Median = ", unname(q[3]),
  ", Q3 = ", unname(q[4]),
  ", Max = ", unname(q[5])
)

# Print to console
cat(summary_line, "\n")

# Save to text file
writeLines(
  summary_line,
  con = file.path(output_dir, "read_v2_umgc_ua_22_24_item_count_quartiles.txt")
)
# ------------------------------------------------------------
# Section 4. Save outputs
# ------------------------------------------------------------

save_both(read_v2_umgc_ua_22_24, output_dir, "read_v2_umgc_ua_22_24")

write.csv(
  cat_descriptives_df,
  file.path(output_dir, "categorical_descriptives_read_v2_umgc_ua_22_24.csv"),
  row.names = FALSE
)

write.csv(
  num_descriptives,
  file.path(output_dir, "numeric_descriptives_read_v2_umgc_ua_22_24.csv"),
  row.names = FALSE
)

write.csv(
  sample_size_report$overall_counts,
  file.path(output_dir, "item_sample_size_overall.csv"),
  row.names = FALSE
)

write.csv(
  sample_size_report$by_demo_counts,
  file.path(output_dir, "item_sample_size_by_demo.csv"),
  row.names = FALSE
)

saveRDS(sample_size_report, file.path(output_dir, "item_sample_size_report.rds"))
saveRDS(qa_summary, file.path(output_dir, "qa_summary.rds"))

write.csv(
  tibble(metric = names(qa_summary), value = unlist(qa_summary)),
  file.path(output_dir, "qa_summary.csv"),
  row.names = FALSE
)

ggsave(file.path(output_dir, "age_histogram.png"), p_age_hist, width = 7, height = 5, dpi = 300)
ggsave(file.path(output_dir, "transfer_histogram.png"), p_transfer_hist, width = 7, height = 5, dpi = 300)
ggsave(file.path(output_dir, "readTime_histogram.png"), p_readTime_hist, width = 7, height = 5, dpi = 300)
ggsave(file.path(output_dir, "age_boxplot.png"), p_age_box, width = 5, height = 5, dpi = 300)
ggsave(file.path(output_dir, "transfer_boxplot.png"), p_transfer_box, width = 5, height = 5, dpi = 300)
ggsave(file.path(output_dir, "readTime_boxplot.png"), p_readTime_box, width = 5, height = 5, dpi = 300)