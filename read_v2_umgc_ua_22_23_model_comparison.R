# read_v2_umgc_ua_22_23_model_comparison.R
# ============================================================
# DAACS Reading: Final Model-Comparison Workflow
# Final frozen analytic dataset:
#   read_v2_umgc_ua_22_23_final
#
# Final analytic sample:
# - student-only records
# - umgc1 trial administrations removed
# - college recoded to ua / umgc
# - age kept continuous
# - all 180 reading items retained for first-stage comparison
#
# Main comparison:
#   Rasch / 1PL vs 2PL vs 2PL bifactor
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(readr)
  library(tibble)
  library(purrr)
  library(ggplot2)
  library(mirt)
})

# ------------------------------------------------------------
# 0. Configuration
# ------------------------------------------------------------

output_dir <- "C:/Users/orosc/OneDrive/Read/read_v2_umgc_ua_22_23_model_comparison_outputs"
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# ------------------------------------------------------------
# 1. Load / verify final frozen analytic dataset
# ------------------------------------------------------------

# If needed:
read_v2_umgc_ua_22_23_final <- 
  readRDS("C:/Users/orosc/OneDrive/Read/read_v2_umgc_ua_22_23_final_outputs/read_v2_umgc_ua_22_23_final.rds")
dat <- read_v2_umgc_ua_22_23_final

stopifnot(is.data.frame(dat))
stopifnot("college" %in% names(dat))
stopifnot("age" %in% names(dat))

# Item columns: preserve full names exactly as stored
item_cols <- grep("^Q\\d{3}", names(dat), value = TRUE)

cat("N rows:", nrow(dat), "\n")
cat("N item cols:", length(item_cols), "\n")
print(table(dat$college, useNA = "always"))
print(table(dat$wave, useNA = "always"))

stopifnot(length(item_cols) == 180)

# ------------------------------------------------------------
# 2. Student-level and item-only objects
# ------------------------------------------------------------

# Student-level covariates for merges and subgroup summaries
student_covars <- dat %>%
  select(
    DAACS_ID, global_id, college, wave, age,
    gender, ethnicity, military, pell, transfer,
    readTime
  )

# Items-only matrix for IRT modeling
read_items <- dat %>%
  select(all_of(item_cols)) %>%
  mutate(across(everything(), ~ as.integer(.x)))

stopifnot(nrow(student_covars) == nrow(read_items))

# ------------------------------------------------------------
# 3. Item lookup from item names
# Correct reading domains:
#   s  = structure
#   in = inference
#   id = ideas
#   p  = purpose
#   l  = language
# ------------------------------------------------------------

item_lookup <- tibble(QID = item_cols) %>%
  mutate(
    testlet = str_extract(QID, "\\d+$"),
    domain_code = str_match(QID, "^Q\\d{3}([A-Za-z]+)\\d+$")[, 2],
    domain = case_when(
      domain_code == "s"  ~ "structure",
      domain_code == "in" ~ "inference",
      domain_code == "id" ~ "ideas",
      domain_code == "p"  ~ "purpose",
      domain_code == "l"  ~ "language",
      TRUE ~ NA_character_
    )
  )

write_csv(item_lookup, file.path(output_dir, "item_lookup_read_180.csv"))

# ------------------------------------------------------------
# 4. Item-count summaries for this frozen modeling file
# ------------------------------------------------------------

item_counts_overall <- tibble(
  QID = item_cols,
  n_responded = sapply(read_items[item_cols], function(x) sum(!is.na(x))),
  n_wrong = sapply(read_items[item_cols], function(x) sum(x == 0, na.rm = TRUE)),
  n_correct = sapply(read_items[item_cols], function(x) sum(x == 1, na.rm = TRUE))
) %>%
  arrange(QID)

item_count_summary <- item_counts_overall %>%
  summarise(
    min = min(n_responded),
    q1 = unname(quantile(n_responded, 0.25)),
    median = median(n_responded),
    q3 = unname(quantile(n_responded, 0.75)),
    max = max(n_responded),
    mean = mean(n_responded),
    sd = sd(n_responded)
  )

item_counts_by_domain <- item_counts_overall %>%
  left_join(item_lookup, by = "QID") %>%
  group_by(domain) %>%
  summarise(
    n_items = n(),
    min_n = min(n_responded),
    q1_n = unname(quantile(n_responded, 0.25)),
    median_n = median(n_responded),
    q3_n = unname(quantile(n_responded, 0.75)),
    max_n = max(n_responded),
    .groups = "drop"
  ) %>%
  arrange(domain)

item_counts_by_testlet <- item_counts_overall %>%
  left_join(item_lookup, by = "QID") %>%
  group_by(testlet) %>%
  summarise(
    n_items = n(),
    min_n = min(n_responded),
    q1_n = unname(quantile(n_responded, 0.25)),
    median_n = median(n_responded),
    q3_n = unname(quantile(n_responded, 0.75)),
    max_n = max(n_responded),
    .groups = "drop"
  ) %>%
  arrange(as.numeric(testlet))

write_csv(item_counts_overall, file.path(output_dir, "item_sample_size_overall_final_modeling_file.csv"))
write_csv(item_count_summary, file.path(output_dir, "item_count_summary_final_modeling_file.csv"))
write_csv(item_counts_by_domain, file.path(output_dir, "item_counts_by_domain_final_modeling_file.csv"))
write_csv(item_counts_by_testlet, file.path(output_dir, "item_counts_by_testlet_final_modeling_file.csv"))

# ------------------------------------------------------------
# 5. Model 1: Rasch / 1PL unidimensional
# ------------------------------------------------------------

set.seed(2026)

mod_rasch <- mirt(
  data = read_items,
  model = 1,
  itemtype = "Rasch",
  verbose = TRUE
)

saveRDS(mod_rasch, file.path(output_dir, "mod_rasch_read180.rds"))

theta_rasch <- fscores(
  mod_rasch,
  method = "EAP",
  full.scores = TRUE,
  full.scores.SE = TRUE
) %>%
  as.data.frame()

names(theta_rasch)[1:2] <- c("theta_rasch", "SE_theta_rasch")

# ------------------------------------------------------------
# 6. Model 2: Unidimensional 2PL
# ------------------------------------------------------------

set.seed(2026)

mod_2pl <- mirt(
  data = read_items,
  model = 1,
  itemtype = "2PL",
  verbose = TRUE
)

saveRDS(mod_2pl, file.path(output_dir, "mod_2pl_read180.rds"))

theta_2pl <- fscores(
  mod_2pl,
  method = "EAP",
  full.scores = TRUE,
  full.scores.SE = TRUE
) %>%
  as.data.frame()

names(theta_2pl)[1:2] <- c("theta_2pl", "SE_theta_2pl")

# ------------------------------------------------------------
# 7. Model 3: 2PL bifactor
# One general factor + 30 specific passage/testlet factors
#
# IMPORTANT:
# If you already have working bifactor syntax from your earlier
# reading analyses, use that here.
# The structure below assumes each block of 6 consecutive items
# belongs to one testlet.
# ------------------------------------------------------------

# Build specific-factor lines for 30 testlets
specific_lines <- map_chr(1:30, function(i) {
  start_idx <- (i - 1) * 6 + 1
  end_idx <- i * 6
  paste0("S", i, " = ", start_idx, "-", end_idx)
})

# Print so you can inspect
cat(paste(specific_lines, collapse = "\n"), "\n")

set.seed(2026)

mod_bifactor_2pl <- bfactor(
  data = read_items,
  model = specific_lines,
  itemtype = "2PL",
  verbose = TRUE
)

saveRDS(mod_bifactor_2pl, file.path(output_dir, "mod_bifactor_2pl_read180.rds"))

theta_bifactor <- fscores(
  mod_bifactor_2pl,
  method = "EAP",
  full.scores = TRUE,
  full.scores.SE = TRUE
) %>%
  as.data.frame()

# Inspect names before relying on them
print(names(theta_bifactor))

# Usually the first factor is the general factor
names(theta_bifactor)[1] <- "theta_bifactor_G"

# Rename first SE column if present and identifiable
if (ncol(theta_bifactor) >= 2) {
  # Keep only if the second column is the SE for G in your output
  # If not, adjust after inspection
  names(theta_bifactor)[2] <- "SE_theta_bifactor_G"
}

# ------------------------------------------------------------
# 8. Comparative model-fit evaluation
# Direct extraction from mirt Fit slot
# ------------------------------------------------------------

fit_table <- tibble::tibble(
  model = c("Rasch", "2PL", "2PL_bifactor"),
  npar = c(
    tryCatch(sum(mod2values(mod_rasch)$est), error = function(e) NA_real_),
    tryCatch(sum(mod2values(mod_2pl)$est), error = function(e) NA_real_),
    tryCatch(sum(mod2values(mod_bifactor_2pl)$est), error = function(e) NA_real_)
  ),
  logLik = c(
    tryCatch(mod_rasch@Fit$logLik, error = function(e) NA_real_),
    tryCatch(mod_2pl@Fit$logLik, error = function(e) NA_real_),
    tryCatch(mod_bifactor_2pl@Fit$logLik, error = function(e) NA_real_)
  ),
  AIC = c(
    tryCatch(mod_rasch@Fit$AIC, error = function(e) NA_real_),
    tryCatch(mod_2pl@Fit$AIC, error = function(e) NA_real_),
    tryCatch(mod_bifactor_2pl@Fit$AIC, error = function(e) NA_real_)
  ),
  SABIC = c(
    tryCatch(mod_rasch@Fit$SABIC, error = function(e) NA_real_),
    tryCatch(mod_2pl@Fit$SABIC, error = function(e) NA_real_),
    tryCatch(mod_bifactor_2pl@Fit$SABIC, error = function(e) NA_real_)
  ),
  HQ = c(
    tryCatch(mod_rasch@Fit$HQ, error = function(e) NA_real_),
    tryCatch(mod_2pl@Fit$HQ, error = function(e) NA_real_),
    tryCatch(mod_bifactor_2pl@Fit$HQ, error = function(e) NA_real_)
  ),
  BIC = c(
    tryCatch(mod_rasch@Fit$BIC, error = function(e) NA_real_),
    tryCatch(mod_2pl@Fit$BIC, error = function(e) NA_real_),
    tryCatch(mod_bifactor_2pl@Fit$BIC, error = function(e) NA_real_)
  ),
  converged = c(
    tryCatch(isTRUE(extract.mirt(mod_rasch, "converged")), error = function(e) NA),
    tryCatch(isTRUE(extract.mirt(mod_2pl, "converged")), error = function(e) NA),
    tryCatch(isTRUE(extract.mirt(mod_bifactor_2pl, "converged")), error = function(e) NA)
  )
)

print(fit_table)

readr::write_csv(
  fit_table,
  file.path(output_dir, "model_fit_comparison_read180.csv")
)

# ------------------------------------------------------------
# 8A. Information-criterion ranking
# ------------------------------------------------------------

fit_ranking_AIC <- fit_table %>%
  arrange(AIC)

fit_ranking_BIC <- fit_table %>%
  arrange(BIC)

print(fit_ranking_AIC)
print(fit_ranking_BIC)

readr::write_csv(
  fit_ranking_AIC,
  file.path(output_dir, "model_fit_ranking_AIC_read180.csv")
)

readr::write_csv(
  fit_ranking_BIC,
  file.path(output_dir, "model_fit_ranking_BIC_read180.csv")
)

# ------------------------------------------------------------
# 8B. Nested likelihood-ratio comparison: Rasch vs 2PL
# ------------------------------------------------------------

rasch_vs_2pl_LRT <- tryCatch(
  anova(mod_rasch, mod_2pl),
  error = function(e) NULL
)

if (!is.null(rasch_vs_2pl_LRT)) {
  print(rasch_vs_2pl_LRT)
  capture.output(
    rasch_vs_2pl_LRT,
    file = file.path(output_dir, "rasch_vs_2pl_LRT.txt")
  )
}

# ------------------------------------------------------------
# 9. Merge theta outputs with student covariates
# ------------------------------------------------------------

theta_df <- bind_cols(
  student_covars,
  theta_rasch,
  theta_2pl,
  theta_bifactor
)

write_csv(theta_df, file.path(output_dir, "theta_scores_all_models_read180.csv"))

# ------------------------------------------------------------
# 10. Pairwise theta comparisons
# ------------------------------------------------------------

theta_df <- theta_df %>%
  mutate(
    diff_rasch_2pl = theta_rasch - theta_2pl,
    absdiff_rasch_2pl = abs(diff_rasch_2pl),
    
    diff_rasch_bif = theta_rasch - theta_bifactor_G,
    absdiff_rasch_bif = abs(diff_rasch_bif),
    
    diff_2pl_bif = theta_2pl - theta_bifactor_G,
    absdiff_2pl_bif = abs(diff_2pl_bif),
    
    avg_rasch_2pl = (theta_rasch + theta_2pl) / 2,
    avg_rasch_bif = (theta_rasch + theta_bifactor_G) / 2,
    avg_2pl_bif = (theta_2pl + theta_bifactor_G) / 2
  )

theta_correlations <- tibble(
  comparison = c("rasch_vs_2pl", "rasch_vs_bifactorG", "2pl_vs_bifactorG"),
  correlation = c(
    cor(theta_df$theta_rasch, theta_df$theta_2pl, use = "pairwise.complete.obs"),
    cor(theta_df$theta_rasch, theta_df$theta_bifactor_G, use = "pairwise.complete.obs"),
    cor(theta_df$theta_2pl, theta_df$theta_bifactor_G, use = "pairwise.complete.obs")
  )
)

theta_diff_summary <- tibble(
  comparison = c("rasch_vs_2pl", "rasch_vs_bifactorG", "2pl_vs_bifactorG"),
  mean_absdiff = c(
    mean(theta_df$absdiff_rasch_2pl, na.rm = TRUE),
    mean(theta_df$absdiff_rasch_bif, na.rm = TRUE),
    mean(theta_df$absdiff_2pl_bif, na.rm = TRUE)
  ),
  median_absdiff = c(
    median(theta_df$absdiff_rasch_2pl, na.rm = TRUE),
    median(theta_df$absdiff_rasch_bif, na.rm = TRUE),
    median(theta_df$absdiff_2pl_bif, na.rm = TRUE)
  ),
  p90_absdiff = c(
    quantile(theta_df$absdiff_rasch_2pl, 0.90, na.rm = TRUE),
    quantile(theta_df$absdiff_rasch_bif, 0.90, na.rm = TRUE),
    quantile(theta_df$absdiff_2pl_bif, 0.90, na.rm = TRUE)
  ),
  p95_absdiff = c(
    quantile(theta_df$absdiff_rasch_2pl, 0.95, na.rm = TRUE),
    quantile(theta_df$absdiff_rasch_bif, 0.95, na.rm = TRUE),
    quantile(theta_df$absdiff_2pl_bif, 0.95, na.rm = TRUE)
  )
)

write_csv(theta_correlations, file.path(output_dir, "theta_correlations_read180.csv"))
write_csv(theta_diff_summary, file.path(output_dir, "theta_diff_summary_read180.csv"))

# ------------------------------------------------------------
# 11. Scatterplots
# ------------------------------------------------------------

p_rasch_2pl <- ggplot(theta_df, aes(x = theta_rasch, y = theta_2pl)) +
  geom_point(alpha = 0.25) +
  geom_smooth(method = "lm", se = FALSE) +
  labs(
    title = "Rasch vs 2PL Theta",
    x = "Rasch theta",
    y = "2PL theta"
  ) +
  theme_minimal()

p_rasch_bif <- ggplot(theta_df, aes(x = theta_rasch, y = theta_bifactor_G)) +
  geom_point(alpha = 0.25) +
  geom_smooth(method = "lm", se = FALSE) +
  labs(
    title = "Rasch vs Bifactor General Theta",
    x = "Rasch theta",
    y = "Bifactor general theta"
  ) +
  theme_minimal()

p_2pl_bif <- ggplot(theta_df, aes(x = theta_2pl, y = theta_bifactor_G)) +
  geom_point(alpha = 0.25) +
  geom_smooth(method = "lm", se = FALSE) +
  labs(
    title = "2PL vs Bifactor General Theta",
    x = "2PL theta",
    y = "Bifactor general theta"
  ) +
  theme_minimal()

ggsave(file.path(output_dir, "scatter_rasch_vs_2pl.png"), p_rasch_2pl, width = 8, height = 6, dpi = 300)
ggsave(file.path(output_dir, "scatter_rasch_vs_bifactorG.png"), p_rasch_bif, width = 8, height = 6, dpi = 300)
ggsave(file.path(output_dir, "scatter_2pl_vs_bifactorG.png"), p_2pl_bif, width = 8, height = 6, dpi = 300)

# ------------------------------------------------------------
# 12. Difference-vs-average plots
# ------------------------------------------------------------

p_ba_rasch_2pl <- ggplot(theta_df, aes(x = avg_rasch_2pl, y = diff_rasch_2pl)) +
  geom_point(alpha = 0.25) +
  geom_hline(yintercept = mean(theta_df$diff_rasch_2pl, na.rm = TRUE), linetype = "dashed") +
  labs(title = "Difference vs Average: Rasch - 2PL", x = "Average theta", y = "Difference") +
  theme_minimal()

p_ba_rasch_bif <- ggplot(theta_df, aes(x = avg_rasch_bif, y = diff_rasch_bif)) +
  geom_point(alpha = 0.25) +
  geom_hline(yintercept = mean(theta_df$diff_rasch_bif, na.rm = TRUE), linetype = "dashed") +
  labs(title = "Difference vs Average: Rasch - Bifactor G", x = "Average theta", y = "Difference") +
  theme_minimal()

p_ba_2pl_bif <- ggplot(theta_df, aes(x = avg_2pl_bif, y = diff_2pl_bif)) +
  geom_point(alpha = 0.25) +
  geom_hline(yintercept = mean(theta_df$diff_2pl_bif, na.rm = TRUE), linetype = "dashed") +
  labs(title = "Difference vs Average: 2PL - Bifactor G", x = "Average theta", y = "Difference") +
  theme_minimal()

ggsave(file.path(output_dir, "diffavg_rasch_vs_2pl.png"), p_ba_rasch_2pl, width = 8, height = 6, dpi = 300)
ggsave(file.path(output_dir, "diffavg_rasch_vs_bifactorG.png"), p_ba_rasch_bif, width = 8, height = 6, dpi = 300)
ggsave(file.path(output_dir, "diffavg_2pl_vs_bifactorG.png"), p_ba_2pl_bif, width = 8, height = 6, dpi = 300)

# ------------------------------------------------------------
# 13. Subgroup summaries of theta differences
# ------------------------------------------------------------

group_vars <- c("college", "wave", "gender", "ethnicity", "military", "pell")

theta_diff_by_group <- map_dfr(group_vars, function(v) {
  theta_df %>%
    mutate(group_value = as.character(.data[[v]])) %>%
    group_by(grouping_variable = v, group_value) %>%
    summarise(
      n = n(),
      mean_diff_rasch_2pl = mean(diff_rasch_2pl, na.rm = TRUE),
      mean_absdiff_rasch_2pl = mean(absdiff_rasch_2pl, na.rm = TRUE),
      
      mean_diff_rasch_bif = mean(diff_rasch_bif, na.rm = TRUE),
      mean_absdiff_rasch_bif = mean(absdiff_rasch_bif, na.rm = TRUE),
      
      mean_diff_2pl_bif = mean(diff_2pl_bif, na.rm = TRUE),
      mean_absdiff_2pl_bif = mean(absdiff_2pl_bif, na.rm = TRUE),
      .groups = "drop"
    )
})

write_csv(theta_diff_by_group, file.path(output_dir, "theta_diff_by_group_read180.csv"))

# ------------------------------------------------------------
# 14. Continuous predictors: age and transfer
# ------------------------------------------------------------

theta_diff_continuous <- tibble(
  predictor = c("age", "transfer"),
  cor_absdiff_rasch_2pl = c(
    cor(theta_df$age, theta_df$absdiff_rasch_2pl, use = "pairwise.complete.obs"),
    cor(theta_df$transfer, theta_df$absdiff_rasch_2pl, use = "pairwise.complete.obs")
  ),
  cor_absdiff_rasch_bif = c(
    cor(theta_df$age, theta_df$absdiff_rasch_bif, use = "pairwise.complete.obs"),
    cor(theta_df$transfer, theta_df$absdiff_rasch_bif, use = "pairwise.complete.obs")
  ),
  cor_absdiff_2pl_bif = c(
    cor(theta_df$age, theta_df$absdiff_2pl_bif, use = "pairwise.complete.obs"),
    cor(theta_df$transfer, theta_df$absdiff_2pl_bif, use = "pairwise.complete.obs")
  )
)

write_csv(theta_diff_continuous, file.path(output_dir, "theta_diff_continuous_correlations_read180.csv"))

p_age_diff <- ggplot(theta_df, aes(x = age, y = absdiff_2pl_bif)) +
  geom_point(alpha = 0.25) +
  geom_smooth(se = FALSE) +
  labs(
    title = "Absolute Theta Difference (2PL vs Bifactor G) by Age",
    x = "Age",
    y = "Absolute theta difference"
  ) +
  theme_minimal()

p_transfer_diff <- ggplot(theta_df, aes(x = transfer, y = absdiff_2pl_bif)) +
  geom_point(alpha = 0.25) +
  geom_smooth(se = FALSE) +
  labs(
    title = "Absolute Theta Difference (2PL vs Bifactor G) by Transfer Credits",
    x = "Transfer credits",
    y = "Absolute theta difference"
  ) +
  theme_minimal()

ggsave(file.path(output_dir, "absdiff_2pl_bif_by_age.png"), p_age_diff, width = 8, height = 6, dpi = 300)
ggsave(file.path(output_dir, "absdiff_2pl_bif_by_transfer.png"), p_transfer_diff, width = 8, height = 6, dpi = 300)

# ------------------------------------------------------------
# 15. Save final merged analysis object
# ------------------------------------------------------------

saveRDS(theta_df, file.path(output_dir, "theta_comparison_analysis_object_read180.rds"))

cat("\nReading model-comparison workflow finished.\n")