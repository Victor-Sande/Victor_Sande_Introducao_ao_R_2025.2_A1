#Passo 1 - Bibliotecas----
library(arrow)
library(readxl)
library(tidyverse)
library(readr)
library(purrr)
library(ggplot2)

#Passo 2 - Tratamento das PNADs ----
#Foi utilizada a visita 1. 

treat_pnad <- function(path, input_path,
                       dictionary_path, output_path, tipo_pnad, db) {
  
  library(magrittr)
  
  dic_correction <- readr::read_table(
    input_path,
    col_names = FALSE,
    skip = 10
  ) %>%
    
    dplyr::filter(stringr::str_starts(stringr::str_trim(X1), "@")) %>%
    dplyr::mutate(
      type = ifelse(substr(X3, 1, 1) == "$", TRUE, FALSE),
      start = stringr::str_replace_all(X1, "@", "") %>% as.numeric(),
      X3 = as.integer(chartr("$", " ", X3)),
      end = start + X3 - 1,
      X2 = stringr::str_to_upper(X2),
      divisor = 1
    ) %>%
    dplyr::select(varname = X2, width = X3, char = type, divisor, start, end)
  
  PNADc <- readr::read_fwf(
    path,
    readr::fwf_positions(
      dic_correction$start,
      dic_correction$end, dic_correction$varname)) %>%
    dplyr::mutate(
      REGIAO = trunc(as.numeric(UF) / 10, 0),
      PAIS = 0
    )
  
  PNADc %>%
    arrow::write_parquet(.,output_path)}



treat_pnad(
  path = "pnadc/dados/input/visita_5/2021/PNADC_2021_visita5.txt",
  input_path = "pnadc/dados/input/visita_5/2021/input_PNADC_2021_visita5.txt",
  dictionary_path = "pnadc/dados/input/visita_5/2021/dicionario_PNADC_microdados_2021_visita5.xls", 
  output_path = "pnadc/dados/output/pnadc_vis1_trat/pnadc_anual_2021.parquet",
  tipo_pnad = "anual",
  db = NULL
)

#Nota: como o arquivo da PNAD é muito grande, não consegui ler automaticamente uma por uma;
    #  por isso, tive de ir alterando as variáveis ano a ano. 
    #  no fim, terei os arquivos padronizados para ler mais rapidamente, e esse poderá ser
    #  automático. 


#Passo 3 - Calcular a taxa de desocupação -----

#=========================
#Importando as PNADs -----
#=========================

caminho <- "pnadc/dados/output/pnadc_vis1_trat"

arquivos <- list.files(path = caminho, pattern = "\\.parquet$", full.names = TRUE)

nomes <- arquivos %>% 
  basename() %>% 
  str_remove("\\.parquet$")

walk2(arquivos, nomes, ~ assign(.y, read_parquet(.x), envir = .GlobalEnv)) 

#Variável de interesse: VD4002

walk(nomes, ~ {
  df <- get(.x) %>% 
    select(ANO, TRIMESTRE, UF, V1032, VD4002)
  assign(.x, df, envir = .GlobalEnv)
})

walk(nomes, ~ {
  df <- get(.x) %>% 
    rename(peso = V1032,
           sit = VD4002) %>% 
    filter(!is.na(sit)) %>% 
    mutate(ocup = sit == 1)
  
  novo_nome <- gsub("pnadc_anual_", "pnadc_step1_", .x)
  assign(novo_nome, df, envir = .GlobalEnv)
})

novos_nomes <- ls(pattern = "^pnadc_step1_\\d{4}$")

#=====================
#Cálculo Para UFS ----
#=====================
walk(novos_nomes, ~ {
  df <- get(.x) %>% 
    mutate(peso = as.numeric(peso)) %>% 
    group_by(UF) %>%
    mutate(
      soma_ocup = sum(if_else(ocup == TRUE, peso, 0)),
      soma_desocup = sum(if_else(ocup == FALSE, peso, 0)),
      total = soma_ocup + soma_desocup
    ) %>%
    mutate(tx_desocup = soma_desocup / total) %>% 
    ungroup() %>% 
    distinct(UF, .keep_all = TRUE)
  
  novo_nome <- gsub("pnadc_step1_", "pnadc_ufs_", .x)
  assign(novo_nome, df, envir = .GlobalEnv)
})

#===========================================
#Cálculo para Regiões, a partir das UFs ----
#===========================================

novos_nomes_2 <- ls(pattern = "^pnadc_ufs_\\d{4}$")

walk(novos_nomes_2, ~ {
  df <- get(.x) %>%
    mutate(
      UF = substr(as.character(UF), 1, 1)  
    ) %>%
    group_by(UF, ANO) %>%
    mutate(
      soma_ocup = sum(soma_ocup, na.rm = TRUE),
      soma_desocup = sum(soma_desocup, na.rm = TRUE),
      total = soma_ocup + soma_desocup
    ) %>%
    mutate(tx_desocup = soma_desocup / total) %>% 
    ungroup() %>% 
    distinct(UF, .keep_all = TRUE)
  
  novo_nome <- gsub("pnadc_ufs_", "pnadc_reg_", .x)
  assign(novo_nome, df, envir = .GlobalEnv)
})

#=======================
#Cálculo pro Brasil ----
#=======================

novos_nomes_3 <- ls(pattern = "^pnadc_reg_\\d{4}$")

walk(novos_nomes_3, ~ {
  df <- get(.x) %>%
    mutate(UF = "0") %>% 
    mutate(
      soma_ocup = sum(soma_ocup, na.rm = TRUE),
      soma_desocup = sum(soma_desocup, na.rm = TRUE),
      total = soma_ocup + soma_desocup
    ) %>%
    mutate(tx_desocup = soma_desocup / total) %>% 
    distinct(UF, .keep_all = TRUE)
  
  novo_nome <- gsub("pnadc_reg_", "pnadc_br_", .x)
  assign(novo_nome, df, envir = .GlobalEnv)
})
#=======================================
#Juntando os recortes para cada ano ---
#=======================================

anos <- ls(pattern = "^pnadc_br_\\d{4}$") %>% 
  str_extract("\\d{4}") %>% 
  sort()

walk(anos, ~ {
  df_ufs <- get(paste0("pnadc_ufs_", .x)) %>% 
    mutate(UF = as.numeric(UF))
  df_reg <- get(paste0("pnadc_reg_", .x)) %>% 
    mutate(UF = as.numeric(UF))
  df_br  <- get(paste0("pnadc_br_", .x)) %>% 
    mutate(UF = as.numeric(UF))
  
  
  df_final <- bind_rows(df_ufs, df_reg, df_br) %>% 
    arrange(UF)
  
  assign(paste0("pnadc_total_", .x), df_final, envir = .GlobalEnv)
})

nomes_totais <- ls(pattern = "^pnadc_total_\\d{4}$")

pnadc_total <- map_dfr(nomes_totais, ~ {
  df <- get(.x)
  ano <- str_extract(.x, "\\d{4}")
  df$ano <- as.integer(ano)
  df <- df 
}) %>% 
  arrange(UF) %>% 
  ungroup()

pnadc_total <- pnadc_total %>%
  select(ano, UF, soma_ocup, soma_desocup, total, tx_desocup) %>% 
  mutate(tx_desocup = tx_desocup * 100) %>% 
  rename(codigo = UF)

#===================
#tabela auxiliar ---
#===================

#o intuito da tabela é relacionar o código com o respectivo nome, para assim criar os gráficos
#as legendas corretas

tab_aux <- read_xlsx("pnadc/dados/input/tab_aux/tabela_aux_codigo_nome.xlsx")
  
pnadc_total_join <- pnadc_total %>% 
  left_join(tab_aux, by = "codigo")

pnadc_total_rename <- pnadc_total_join %>% 
  rename(nome_UF = nome,
         codigo_UF = codigo) %>% 
  select(ano, codigo_UF, nome_UF, soma_ocup, soma_desocup, total, tx_desocup) 
 
write.csv(pnadc_total_rename, "pnadc/dados/output/perc_pnadc_desocupacao.csv", fileEncoding = "UTF-8", row.names = FALSE)

#Passo 4 - Gráficos ----


#aqui, limpei o environment e importei a tabela final (pnadc_total_rename).

df <- read.csv("pnadc/dados/output/perc_pnadc_desocupacao.csv")

#o primeiro gráfico será a série histórica para as regiões.

dfreg <- df %>% 
  filter(codigo_UF > 0 & codigo_UF < 10)

ggplot(dfreg, aes(x = ano, y = tx_desocup, color = nome_UF, group = nome_UF)) +
  geom_line(size = 1) +
  geom_point() +
  scale_x_continuous(breaks = seq(min(df$ano), max(df$ano), by = 1)) +
  labs(
    title = "Taxa de desocupação por região",
    x = "Ano",
    y = "Taxa (%)",
    color = NULL
  ) +
  theme_minimal() +
  theme(
    plot.title = element_text(hjust = 0.5, face = "bold"),
    legend.position = "bottom"  
  )

#o segundo gráfico será para as UFs da Região sudeste (e a região), para o ano de 2024.. 

dfsudeste <- df %>% 
  filter(codigo_UF > 30 & codigo_UF < 40 | codigo_UF == 3,
         ano == 2024)






























