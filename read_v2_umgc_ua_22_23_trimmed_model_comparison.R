# read_v2_umgc_ua_22_23_trimmed_model_comparison.R
# ============================================================
# DAACS Reading: Trimmed-bank model comparison
# Models:
#   1. Rasch / 1PL
#   2. Unidimensional 2PL
#   3. 2PL bifactor
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

output_dir <- "C:/Users/orosc/OneDrive/Read/read_v2_umgc_ua_22_23_trimmed_model_comparison_outputs"
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# ------------------------------------------------------------
# 1. Load final dataset and trimmed item pool
# ------------------------------------------------------------

read_v2_umgc_ua_22_23_final <- readRDS("C:/Users/orosc/OneDrive/Read/read_v2_umgc_ua_22_23_final_outputs/read_v2_umgc_ua_22_23_final.rds")

dat <- read_v2_umgc_ua_22_23_final

trimmed_item_pool <- read_csv(
  file.path("C:/Users/orosc/OneDrive/Read/read_v2_umgc_ua_22_23_model_180trimming_outputs/trimmed_item_pool_read.csv"),
  show_col_types = FALSE
)

trimmed_items <- trimmed_item_pool %>%
  pull(QID) %>%
  as.character()

stopifnot(length(trimmed_items) > 0)

# Keep only trimmed item columns
read_items_trimmed <- dat %>%
  select(all_of(trimmed_items)) %>%
  mutate(across(everything(), ~ as.integer(.x)))

# Student-level covariates
student_covars <- dat %>%
  select(
    DAACS_ID, global_id, college, wave, age,
    gender, ethnicity, military, pell, transfer,
    readTime
  )

stopifnot(nrow(read_items_trimmed) == nrow(student_covars))

# ------------------------------------------------------------
# 2. Item lookup for trimmed pool
# ------------------------------------------------------------

item_lookup_trimmed <- tibble(QID = trimmed_items) %>%
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

write_csv(
  item_lookup_trimmed,
  file.path(output_dir, "item_lookup_trimmed_read.csv")
)

# ------------------------------------------------------------
# 3. Fit trimmed Rasch
# ------------------------------------------------------------

set.seed(2026)

mod_rasch_trimmed <- mirt(
  data = read_items_trimmed,
  model = 1,
  itemtype = "Rasch",
  verbose = TRUE
)

saveRDS(
  mod_rasch_trimmed,
  file.path(output_dir, "mod_rasch_trimmed_read.rds")
)

# ------------------------------------------------------------
# 4. Fit trimmed 2PL
# ------------------------------------------------------------

set.seed(2026)

mod_2pl_trimmed <- mirt(
  data = read_items_trimmed,
  model = 1,
  itemtype = "2PL",
  verbose = TRUE
)

saveRDS(
  mod_2pl_trimmed,
  file.path(output_dir, "mod_2pl_trimmed_read.rds")
)

# ------------------------------------------------------------
# 5. Fit trimmed 2PL bifactor
# ------------------------------------------------------------
# Build bifactor specific-factor lines from retained items.
# Each testlet factor loads on the retained items from that testlet.
# Testlets with fewer than 2 retained items are dropped from the
# specific-factor structure.
# ------------------------------------------------------------

testlet_membership <- split(trimmed_items, str_extract(trimmed_items, "\\d+$"))

# positions in the trimmed item matrix
item_index_lookup <- tibble(
  QID = trimmed_items,
  item_index = seq_along(trimmed_items),
  testlet = str_extract(QID, "\\d+$")
)

specific_lines_trimmed <- item_index_lookup %>%
  group_by(testlet) %>%
  summarise(
    item_indices = list(item_index),
    n_items = n(),
    .groups = "drop"
  ) %>%
  filter(n_items >= 2) %>%
  mutate(
    factor_name = paste0("S", row_number()),
    syntax = map2_chr(factor_name, item_indices, function(fac, idx) {
      paste0(fac, " = ", paste(idx, collapse = ","))
    })
  ) %>%
  pull(syntax)

cat(paste(specific_lines_trimmed, collapse = "\n"), "\n")

set.seed(2026)

mod_bifactor_trimmed <- bfactor(
  data = read_items_trimmed,
  model = specific_lines_trimmed,
  itemtype = "2PL",
  verbose = TRUE
)

saveRDS(
  mod_bifactor_trimmed,
  file.path(output_dir, "mod_bifactor_trimmed_read.rds")
)

# ------------------------------------------------------------
# 6. Fit-comparison table for trimmed models
# ------------------------------------------------------------

fit_table_trimmed <- tibble(
  model = c("Rasch_trimmed", "2PL_trimmed", "2PL_bifactor_trimmed"),
  n_items = c(length(trimmed_items), length(trimmed_items), length(trimmed_items)),
  npar = c(
    tryCatch(sum(mod2values(mod_rasch_trimmed)$est), error = function(e) NA_real_),
    tryCatch(sum(mod2values(mod_2pl_trimmed)$est), error = function(e) NA_real_),
    tryCatch(sum(mod2values(mod_bifactor_trimmed)$est), error = function(e) NA_real_)
  ),
  logLik = c(
    tryCatch(mod_rasch_trimmed@Fit$logLik, error = function(e) NA_real_),
    tryCatch(mod_2pl_trimmed@Fit$logLik, error = function(e) NA_real_),
    tryCatch(mod_bifactor_trimmed@Fit$logLik, error = function(e) NA_real_)
  ),
  AIC = c(
    tryCatch(mod_rasch_trimmed@Fit$AIC, error = function(e) NA_real_),
    tryCatch(mod_2pl_trimmed@Fit$AIC, error = function(e) NA_real_),
    tryCatch(mod_bifactor_trimmed@Fit$AIC, error = function(e) NA_real_)
  ),
  SABIC = c(
    tryCatch(mod_rasch_trimmed@Fit$SABIC, error = function(e) NA_real_),
    tryCatch(mod_2pl_trimmed@Fit$SABIC, error = function(e) NA_real_),
    tryCatch(mod_bifactor_trimmed@Fit$SABIC, error = function(e) NA_real_)
  ),
  HQ = c(
    tryCatch(mod_rasch_trimmed@Fit$HQ, error = function(e) NA_real_),
    tryCatch(mod_2pl_trimmed@Fit$HQ, error = function(e) NA_real_),
    tryCatch(mod_bifactor_trimmed@Fit$HQ, error = function(e) NA_real_)
  ),
  BIC = c(
    tryCatch(mod_rasch_trimmed@Fit$BIC, error = function(e) NA_real_),
    tryCatch(mod_2pl_trimmed@Fit$BIC, error = function(e) NA_real_),
    tryCatch(mod_bifactor_trimmed@Fit$BIC, error = function(e) NA_real_)
  ),
  converged = c(
    tryCatch(isTRUE(extract.mirt(mod_rasch_trimmed, "converged")), error = function(e) NA),
    tryCatch(isTRUE(extract.mirt(mod_2pl_trimmed, "converged")), error = function(e) NA),
    tryCatch(isTRUE(extract.mirt(mod_bifactor_trimmed, "converged")), error = function(e) NA)
  )
)

write_csv(
  fit_table_trimmed,
  file.path(output_dir, "model_fit_comparison_trimmed_read.csv")
)

print(fit_table_trimmed)

fit_ranking_trimmed_AIC <- fit_table_trimmed %>% arrange(AIC)
fit_ranking_trimmed_BIC <- fit_table_trimmed %>% arrange(BIC)

write_csv(
  fit_ranking_trimmed_AIC,
  file.path(output_dir, "model_fit_ranking_trimmed_AIC_read.csv")
)

write_csv(
  fit_ranking_trimmed_BIC,
  file.path(output_dir, "model_fit_ranking_trimmed_BIC_read.csv")
)

# Nested LRT: trimmed Rasch vs trimmed 2PL
rasch_vs_2pl_trimmed_LRT <- tryCatch(
  anova(mod_rasch_trimmed, mod_2pl_trimmed),
  error = function(e) NULL
)

if (!is.null(rasch_vs_2pl_trimmed_LRT)) {
  capture.output(
    rasch_vs_2pl_trimmed_LRT,
    file = file.path(output_dir, "rasch_vs_2pl_trimmed_LRT.txt")
  )
  print(rasch_vs_2pl_trimmed_LRT)
}

# ------------------------------------------------------------
# 7. Extract theta scores from trimmed models
# ------------------------------------------------------------

theta_rasch_trimmed <- fscores(
  mod_rasch_trimmed,
  method = "EAP",
  full.scores = TRUE,
  full.scores.SE = TRUE
) %>%
  as.data.frame()

names(theta_rasch_trimmed)[1:2] <- c("theta_rasch_trimmed", "SE_theta_rasch_trimmed")

theta_2pl_trimmed <- fscores(
  mod_2pl_trimmed,
  method = "EAP",
  full.scores = TRUE,
  full.scores.SE = TRUE
) %>%
  as.data.frame()

names(theta_2pl_trimmed)[1:2] <- c("theta_2pl_trimmed", "SE_theta_2pl_trimmed")

theta_bifactor_trimmed <- fscores(
  mod_bifactor_trimmed,
  method = "EAP",
  full.scores = TRUE,
  full.scores.SE = TRUE
) %>%
  as.data.frame()

print(names(theta_bifactor_trimmed))

# General factor should be the first factor
names(theta_bifactor_trimmed)[1] <- "theta_bifactor_trimmed_G"

# Rename first SE column if identifiable
if ("SE_G" %in% names(theta_bifactor_trimmed)) {
  names(theta_bifactor_trimmed)[names(theta_bifactor_trimmed) == "SE_G"] <- "SE_theta_bifactor_trimmed_G"
} else if (ncol(theta_bifactor_trimmed) >= 2) {
  names(theta_bifactor_trimmed)[2] <- "SE_theta_bifactor_trimmed_G"
}

# ------------------------------------------------------------
# 8. Merge trimmed theta outputs
# ------------------------------------------------------------

theta_trimmed_df <- bind_cols(
  student_covars,
  theta_rasch_trimmed,
  theta_2pl_trimmed,
  theta_bifactor_trimmed
)

write_csv(
  theta_trimmed_df,
  file.path(output_dir, "theta_scores_all_models_trimmed_read.csv")
)

# ------------------------------------------------------------
# 8. Comparative model-fit evaluation
# This section evaluates comparative model fit.
# Later sections evaluate whether model choice materially changes
# overall theta estimates.
# ------------------------------------------------------------

mods <- list(
  Rasch = mod_rasch_trimmed,
  `2PL` = mod_2pl_trimmed,
  `2PL_bifactor` = mod_bifactor_trimmed
)

safe_m2_extract <- function(mod) {
  tryCatch(
    {
      out <- M2(mod)
      tibble(
        M2 = if ("M2" %in% names(out)) as.numeric(out[["M2"]]) else NA_real_,
        df_M2 = if ("df" %in% names(out)) as.numeric(out[["df"]]) else NA_real_,
        p_M2 = if ("p" %in% names(out)) as.numeric(out[["p"]]) else NA_real_,
        RMSEA_M2 = if ("RMSEA" %in% names(out)) as.numeric(out[["RMSEA"]]) else NA_real_,
        SRMSR_M2 = if ("SRMSR" %in% names(out)) as.numeric(out[["SRMSR"]]) else NA_real_,
        TLI_M2 = if ("TLI" %in% names(out)) as.numeric(out[["TLI"]]) else NA_real_,
        CFI_M2 = if ("CFI" %in% names(out)) as.numeric(out[["CFI"]]) else NA_real_
      )
    },
    error = function(e) {
      tibble(
        M2 = NA_real_,
        df_M2 = NA_real_,
        p_M2 = NA_real_,
        RMSEA_M2 = NA_real_,
        SRMSR_M2 = NA_real_,
        TLI_M2 = NA_real_,
        CFI_M2 = NA_real_
      )
    }
  )
}

safe_mod_stats <- function(mod, model_name) {
  # parameter count
  coef_list <- tryCatch(coef(mod, simplify = TRUE), error = function(e) NULL)
  
  n_items_par <- tryCatch(
    if (!is.null(coef_list) && "items" %in% names(coef_list)) {
      sum(!is.na(coef_list$items))
    } else {
      NA_integer_
    },
    error = function(e) NA_integer_
  )
  
  n_means_par <- tryCatch(
    if (!is.null(coef_list) && "means" %in% names(coef_list)) length(coef_list$means) else 0L,
    error = function(e) NA_integer_
  )
  
  n_cov_par <- tryCatch(
    if (!is.null(coef_list) && "cov" %in% names(coef_list)) sum(!is.na(coef_list$cov)) else 0L,
    error = function(e) NA_integer_
  )
  
  npar_est <- tryCatch(
    n_items_par + n_means_par + n_cov_par,
    error = function(e) NA_integer_
  )
  
  tibble(
    model = model_name,
    npar_est = npar_est,
    logLik = tryCatch(as.numeric(logLik(mod)), error = function(e) NA_real_),
    AIC = tryCatch(as.numeric(AIC(mod)), error = function(e) NA_real_),
    BIC = tryCatch(as.numeric(BIC(mod)), error = function(e) NA_real_),
    SABIC = tryCatch(as.numeric(M2(mod, calcNull = FALSE)[["SABIC"]]), error = function(e) NA_real_),
    converged = tryCatch(isTRUE(extract.mirt(mod, "converged")), error = function(e) NA)
  ) %>%
    bind_cols(safe_m2_extract(mod))
}

model_fit_comparison <- purrr::imap_dfr(mods, safe_mod_stats)

write_csv(
  model_fit_comparison,
  file.path(output_dir, "model_fit_comparison_read180.csv")
)

print(model_fit_comparison)

# ------------------------------------------------------------
# 8A. Nested likelihood-ratio comparison: Rasch vs 2PL
# Rasch is nested within 2PL, so this comparison is appropriate.
# ------------------------------------------------------------

rasch_vs_2pl_LRT <- tryCatch(
  anova(mod_rasch_trimmed, mod_2pl_trimmed),
  error = function(e) NULL
)

if (!is.null(rasch_vs_2pl_LRT)) {
  capture.output(
    rasch_vs_2pl_LRT,
    file = file.path(output_dir, "rasch_vs_2pl_LRT.txt")
  )
  print(rasch_vs_2pl_LRT)
}

# ------------------------------------------------------------
# 8B. Simple ranking by information criteria
# Smaller AIC / BIC / SABIC indicate better relative fit
# ------------------------------------------------------------

model_fit_ranking <- model_fit_comparison %>%
  arrange(BIC, AIC)

write_csv(
  model_fit_ranking,
  file.path(output_dir, "model_fit_ranking_read180.csv")
)

print(model_fit_ranking)
# ------------------------------------------------------------
# 10. Scatterplots on trimmed bank
# ------------------------------------------------------------

p_trim_rasch_2pl <- ggplot(theta_trimmed_df, aes(x = theta_rasch_trimmed, y = theta_2pl_trimmed)) +
  geom_point(alpha = 0.25) +
  geom_smooth(method = "lm", se = FALSE) +
  labs(
    title = "Trimmed bank: Rasch vs 2PL Theta",
    x = "Rasch theta (trimmed)",
    y = "2PL theta (trimmed)"
  ) +
  theme_minimal()

p_trim_rasch_bif <- ggplot(theta_trimmed_df, aes(x = theta_rasch_trimmed, y = theta_bifactor_trimmed_G)) +
  geom_point(alpha = 0.25) +
  geom_smooth(method = "lm", se = FALSE) +
  labs(
    title = "Trimmed bank: Rasch vs Bifactor General Theta",
    x = "Rasch theta (trimmed)",
    y = "Bifactor general theta (trimmed)"
  ) +
  theme_minimal()

p_trim_2pl_bif <- ggplot(theta_trimmed_df, aes(x = theta_2pl_trimmed, y = theta_bifactor_trimmed_G)) +
  geom_point(alpha = 0.25) +
  geom_smooth(method = "lm", se = FALSE) +
  labs(
    title = "Trimmed bank: 2PL vs Bifactor General Theta",
    x = "2PL theta (trimmed)",
    y = "Bifactor general theta (trimmed)"
  ) +
  theme_minimal()

ggsave(file.path(output_dir, "scatter_trimmed_rasch_vs_2pl.png"), p_trim_rasch_2pl, width = 8, height = 6, dpi = 300)
ggsave(file.path(output_dir, "scatter_trimmed_rasch_vs_bifactorG.png"), p_trim_rasch_bif, width = 8, height = 6, dpi = 300)
ggsave(file.path(output_dir, "scatter_trimmed_2pl_vs_bifactorG.png"), p_trim_2pl_bif, width = 8, height = 6, dpi = 300)

# ------------------------------------------------------------
# 11. Difference-vs-average plots on trimmed bank
# ------------------------------------------------------------

p_trim_ba_rasch_2pl <- ggplot(theta_trimmed_df, aes(x = avg_rasch_2pl_trim, y = diff_rasch_2pl_trim)) +
  geom_point(alpha = 0.25) +
  geom_hline(yintercept = mean(theta_trimmed_df$diff_rasch_2pl_trim, na.rm = TRUE), linetype = "dashed") +
  labs(
    title = "Trimmed bank: Difference vs Average (Rasch - 2PL)",
    x = "Average theta",
    y = "Difference"
  ) +
  theme_minimal()

p_trim_ba_rasch_bif <- ggplot(theta_trimmed_df, aes(x = avg_rasch_bif_trim, y = diff_rasch_bif_trim)) +
  geom_point(alpha = 0.25) +
  geom_hline(yintercept = mean(theta_trimmed_df$diff_rasch_bif_trim, na.rm = TRUE), linetype = "dashed") +
  labs(
    title = "Trimmed bank: Difference vs Average (Rasch - Bifactor G)",
    x = "Average theta",
    y = "Difference"
  ) +
  theme_minimal()

p_trim_ba_2pl_bif <- ggplot(theta_trimmed_df, aes(x = avg_2pl_bif_trim, y = diff_2pl_bif_trim)) +
  geom_point(alpha = 0.25) +
  geom_hline(yintercept = mean(theta_trimmed_df$diff_2pl_bif_trim, na.rm = TRUE), linetype = "dashed") +
  labs(
    title = "Trimmed bank: Difference vs Average (2PL - Bifactor G)",
    x = "Average theta",
    y = "Difference"
  ) +
  theme_minimal()

ggsave(file.path(output_dir, "diffavg_trimmed_rasch_vs_2pl.png"), p_trim_ba_rasch_2pl, width = 8, height = 6, dpi = 300)
ggsave(file.path(output_dir, "diffavg_trimmed_rasch_vs_bifactorG.png"), p_trim_ba_rasch_bif, width = 8, height = 6, dpi = 300)
ggsave(file.path(output_dir, "diffavg_trimmed_2pl_vs_bifactorG.png"), p_trim_ba_2pl_bif, width = 8, height = 6, dpi = 300)

# ------------------------------------------------------------
# 12. Subgroup summaries on trimmed bank
# ------------------------------------------------------------

group_vars <- c("college", "wave", "gender", "ethnicity", "military", "pell")

theta_diff_by_group_trimmed <- purrr::map_dfr(group_vars, function(v) {
  theta_trimmed_df %>%
    mutate(group_value = as.character(.data[[v]])) %>%
    group_by(grouping_variable = v, group_value) %>%
    summarise(
      n = n(),
      mean_diff_rasch_2pl = mean(diff_rasch_2pl_trim, na.rm = TRUE),
      mean_absdiff_rasch_2pl = mean(absdiff_rasch_2pl_trim, na.rm = TRUE),
      
      mean_diff_rasch_bif = mean(diff_rasch_bif_trim, na.rm = TRUE),
      mean_absdiff_rasch_bif = mean(absdiff_rasch_bif_trim, na.rm = TRUE),
      
      mean_diff_2pl_bif = mean(diff_2pl_bif_trim, na.rm = TRUE),
      mean_absdiff_2pl_bif = mean(absdiff_2pl_bif_trim, na.rm = TRUE),
      .groups = "drop"
    )
})

write_csv(
  theta_diff_by_group_trimmed,
  file.path(output_dir, "theta_diff_by_group_trimmed_read.csv")
)

# ------------------------------------------------------------
# 13. Continuous predictors on trimmed bank
# ------------------------------------------------------------

theta_diff_continuous_trimmed <- tibble(
  predictor = c("age", "transfer"),
  cor_absdiff_rasch_2pl = c(
    cor(theta_trimmed_df$age, theta_trimmed_df$absdiff_rasch_2pl_trim, use = "pairwise.complete.obs"),
    cor(theta_trimmed_df$transfer, theta_trimmed_df$absdiff_rasch_2pl_trim, use = "pairwise.complete.obs")
  ),
  cor_absdiff_rasch_bif = c(
    cor(theta_trimmed_df$age, theta_trimmed_df$absdiff_rasch_bif_trim, use = "pairwise.complete.obs"),
    cor(theta_trimmed_df$transfer, theta_trimmed_df$absdiff_rasch_bif_trim, use = "pairwise.complete.obs")
  ),
  cor_absdiff_2pl_bif = c(
    cor(theta_trimmed_df$age, theta_trimmed_df$absdiff_2pl_bif_trim, use = "pairwise.complete.obs"),
    cor(theta_trimmed_df$transfer, theta_trimmed_df$absdiff_2pl_bif_trim, use = "pairwise.complete.obs")
  )
)

write_csv(
  theta_diff_continuous_trimmed,
  file.path(output_dir, "theta_diff_continuous_trimmed_read.csv")
)

# ------------------------------------------------------------
# 14. Save final trimmed-bank analysis object
# ------------------------------------------------------------

saveRDS(
  theta_trimmed_df,
  file.path(output_dir, "theta_comparison_analysis_object_trimmed_read.rds")
)

cat("\nTrimmed-bank model-comparison workflow finished.\n")
cat("Trimmed items retained:", length(trimmed_items), "\n")
cat("Testlet-specific factors retained:", length(specific_lines_trimmed), "\n")