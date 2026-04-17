from dagster import asset, Definitions

# Теги для красных узлов (не реализованный функционал)
UNIMPLEMENTED = {"status": "unimplemented", "dagster/compute_kind": "Not_Implemented"}

# ==========================================
# 1. Извлечение
# ==========================================
# --- Excel ---
@asset(group_name="1_excel")
def obuch_doshkolka(): """Первичка: Excel (Дошколка)"""
@asset(group_name="1_excel")
def naselenie(): """Первичка: Excel (Население)"""

# --- Postgres ---
@asset(group_name="1_postgres")
def obuch_oo(): """Первичка: Postgres (Обуч. ОО-1)"""
@asset(group_name="1_postgres")
def obuch_vpo(): """Первичка: Postgres (Обуч. ВПО-1)"""
@asset(group_name="1_postgres")
def obuch_spo(): """Первичка: Postgres (Обуч. СПО-1)"""
@asset(group_name="1_postgres")
def obuch_pk(): """Первичка: Postgres (Обуч. 1-ПК)"""
@asset(group_name="1_postgres")
def obshagi_vpo(): """Первичка: Postgres (Общаги ВПО-2)"""
@asset(group_name="1_postgres", tags=UNIMPLEMENTED)
def obshagi_spo(): """Первичка: Postgres (Общаги СПО-2)"""
@asset(group_name="1_postgres")
def ped_oo(): """Первичка: Postgres (Педагоги ОО)"""

# --- JSON/CSV ---
@asset(group_name="1_json")
def regions(): """Первичка: Справочник регионов РФ"""
@asset(group_name="1_csv")
def code_programm(): """Первичка: Справочник кодов программ"""

# ==========================================
# 2. Маппинг + Нормализация
# ==========================================
GROUP_n = "2_map_norm"

@asset(group_name=GROUP_n)
def n_obuch_doshkolka(obuch_doshkolka): pass
@asset(group_name=GROUP_n)
def n_naselenie(naselenie): pass
@asset(group_name=GROUP_n)
def n_obuch_oo(obuch_oo): pass
@asset(group_name=GROUP_n)
def n_obuch_vpo(obuch_vpo): pass
@asset(group_name=GROUP_n)
def n_obuch_spo(obuch_spo): pass
@asset(group_name=GROUP_n)
def n_obuch_pk(obuch_pk): pass
@asset(group_name=GROUP_n)
def n_obshagi_vpo(obshagi_vpo): pass
@asset(group_name=GROUP_n, tags=UNIMPLEMENTED)
def n_obshagi_spo(obshagi_spo): """КРАСНЫЙ"""
@asset(group_name=GROUP_n)
def n_ped_kadry(ped_oo): pass

# ==========================================
# 3. СЕРЕБРО L1: АГРЕГАЦИЯ
# ==========================================
GROUP_v_L1 = "3_aregation"

@asset(group_name=GROUP_v_L1)
def a_doshkolka(n_obuch_doshkolka): pass
@asset(group_name=GROUP_v_L1)
def a_naselenie(n_naselenie): pass
@asset(group_name=GROUP_v_L1)
def a_obuch_oo(n_obuch_oo): pass
@asset(group_name=GROUP_v_L1)
def a_obuch_vpo(n_obuch_vpo): pass
@asset(group_name=GROUP_v_L1)
def a_obuch_spo(n_obuch_spo): pass
@asset(group_name=GROUP_v_L1)
def a_obuch_pk(n_obuch_pk): pass
@asset(group_name=GROUP_v_L1)
def a_obshagi_vpo(n_obshagi_vpo): pass
@asset(group_name=GROUP_v_L1, tags=UNIMPLEMENTED)
def a_obshagi_spo(n_obshagi_spo): """КРАСНЫЙ"""
@asset(group_name=GROUP_v_L1)
def a_ped_kadry(n_ped_kadry): pass

# ==========================================
# 4. СЕРЕБРО L2: КОМПОНОВКА (3 подуровня)
# ==========================================
GROUP_L2_STUDENTS = "4_valid_obuch"
@asset(group_name=GROUP_L2_STUDENTS)
def v_naselenie_v_tselom(a_naselenie): pass
@asset(group_name=GROUP_L2_STUDENTS)
def v_1_1(a_doshkolka): pass
@asset(group_name=GROUP_L2_STUDENTS)
def v_1_2(a_obuch_oo): pass
@asset(group_name=GROUP_L2_STUDENTS)
def v_1_3(a_obuch_oo): pass
@asset(group_name=GROUP_L2_STUDENTS)
def v_1_4(a_obuch_oo): pass
@asset(group_name=GROUP_L2_STUDENTS)
def v_2_5_1(a_obuch_oo): pass
@asset(group_name=GROUP_L2_STUDENTS)
def v_2_5_2(a_obuch_oo): pass
@asset(group_name=GROUP_L2_STUDENTS)
def v_2_6(a_obuch_vpo): pass
@asset(group_name=GROUP_L2_STUDENTS)
def v_2_7(a_obuch_vpo): pass
@asset(group_name=GROUP_L2_STUDENTS)
def v_2_8(a_obuch_spo): pass
@asset(group_name=GROUP_L2_STUDENTS, tags=UNIMPLEMENTED)
def v_2_9(a_obuch_spo): """КРАСНЫЙ"""
@asset(group_name=GROUP_L2_STUDENTS)
def v_4_8b_1(a_obuch_pk): pass
@asset(group_name=GROUP_L2_STUDENTS)
def v_4_8b_2(a_obuch_pk): pass
@asset(group_name=GROUP_L2_STUDENTS, tags=UNIMPLEMENTED)
def v_3(a_obuch_pk): """КРАСНЫЙ"""
@asset(group_name=GROUP_L2_STUDENTS, tags=UNIMPLEMENTED)
def v_4_1(a_obuch_pk): """КРАСНЫЙ"""

GROUP_L2_DORMS = "4_valid_obshagi"
@asset(group_name=GROUP_L2_DORMS)
def v_obshagi_vpo_combo(a_obshagi_vpo): """2.6+2.7+2.8+2.9"""
@asset(group_name=GROUP_L2_DORMS, tags=UNIMPLEMENTED)
def v_obshagi_2_5(a_obshagi_spo): """КРАСНЫЙ"""

GROUP_L2_PED = "4_valid_ped"
@asset(group_name=GROUP_L2_PED)
def v_ped_1_2(a_ped_kadry): pass
@asset(group_name=GROUP_L2_PED)
def v_ped_1_3_1_4(a_ped_kadry): pass


# ==========================================
# 5. ЗОЛОТО - ЭТАЛОН
# ==========================================
GROUP_ETALON = "5_etalon"

@asset(group_name=GROUP_ETALON)
def obuch_po_vozrastam(
    v_naselenie_v_tselom, v_1_1, v_1_2, v_1_3, v_1_4,
    v_2_5_1, v_2_5_2, v_2_6, v_2_7, v_2_8, v_2_9,
    v_4_8b_1, v_4_8b_2, v_3, v_4_1
): """Обучающиеся по возрастам"""

@asset(group_name=GROUP_ETALON)
def obshagi(v_obshagi_vpo_combo, v_obshagi_2_5): 
    """Общаги"""

@asset(group_name=GROUP_ETALON)
def ped_kadry(v_ped_1_2, v_ped_1_3_1_4): 
    """Пед кадры"""

@asset(group_name="5_etalon_dict", tags=UNIMPLEMENTED)
def dict_regions(regions): """КРАСНЫЙ"""
@asset(group_name="5_etalon_dict", tags=UNIMPLEMENTED)
def dict_codes(code_programm): """КРАСНЫЙ"""


# ==========================================
# 6. ЗОЛОТО - РАСЧЕТНЫЕ КОЛОНКИ (МЕТРИКИ)
# ==========================================
GROUP_CALC = "6_calculated_columns"

# Метрики для таблицы "Обучающиеся по возрастам"
@asset(group_name=GROUP_CALC)
def metric_dolya_obuch_ot_naselenia(obuch_po_vozrastam): 
    """Доля обучающихся от всего населения в регионе"""

@asset(group_name=GROUP_CALC, tags=UNIMPLEMENTED)
def metric_dolya_nuzhdaushihsya(obuch_po_vozrastam): 
    """Доля нуждающихся от всех обучающихся в регионе (КРАСНЫЙ)"""

# Метрики для таблицы "Общаги"
@asset(group_name=GROUP_CALC)
def metric_dolya_ne_v_remonte(obshagi): 
    """Доля не в ремонте от требующих ремонта"""

@asset(group_name=GROUP_CALC)
def metric_dolya_neobespechennyh(obshagi): 
    """Доля необеспеченных студентов"""

@asset(group_name=GROUP_CALC)
def metric_dolya_trebuushih_remonta(obshagi): 
    """Доля требующих ремонта"""

@asset(group_name=GROUP_CALC)
def metric_dolya_avariynyh(obshagi): 
    """Доля аварийных"""

# Метрики для таблицы "Пед кадры"
@asset(group_name=GROUP_CALC)
def metric_srednyaya_stavka(ped_kadry): 
    """Средняя ставка на учителя"""

@asset(group_name=GROUP_CALC)
def metric_dolya_nezanyatyh(ped_kadry): 
    """Доля незанятых ставок"""


# ==========================================
# 7. ДАШБОРДЫ (Витрины данных)
# ==========================================
GROUP_DASHBOARDS = "7_dashboards"

# Дашборд №1: Собирает метрики по обучающимся
@asset(group_name=GROUP_DASHBOARDS)
def dashboard_obuch_po_vozrastam(
    metric_dolya_obuch_ot_naselenia, 
    metric_dolya_nuzhdaushihsya
): 
    """Дашборд: Обучающиеся по возрастам"""

# Дашборд №2: Собирает метрики по общежитиям
@asset(group_name=GROUP_DASHBOARDS)
def dashboard_obshagi(
    metric_dolya_ne_v_remonte, 
    metric_dolya_neobespechennyh, 
    metric_dolya_trebuushih_remonta, 
    metric_dolya_avariynyh
): 
    """Дашборд: Состояние общежитий"""

# Дашборд №3: Собирает метрики по пед. кадрам
@asset(group_name=GROUP_DASHBOARDS)
def dashboard_ped_kadry(
    metric_srednyaya_stavka, 
    metric_dolya_nezanyatyh
): 
    """Дашборд: Педагогические кадры"""


# ==========================================
# РЕГИСТРАЦИЯ
# ==========================================
defs = Definitions(
    assets=[
        # 1. Извлечение
        obuch_doshkolka, naselenie, obuch_oo, obuch_vpo,
        obuch_spo, obuch_pk, obshagi_vpo, obshagi_spo,
        ped_oo, regions, code_programm,
        
        # 2. Маппинг и валидация
        n_obuch_doshkolka, n_naselenie, n_obuch_oo, n_obuch_vpo,
        n_obuch_spo, n_obuch_pk, n_obshagi_vpo, n_obshagi_spo, n_ped_kadry,
        
        # 3. Агрегация 
        a_doshkolka, a_naselenie, a_obuch_oo, a_obuch_vpo, a_obuch_spo,
        a_obuch_pk, a_obshagi_vpo, a_obshagi_spo, a_ped_kadry,
        
        # 4. Валидация
        v_naselenie_v_tselom, v_1_1, v_1_2, v_1_3, v_1_4,
        v_2_5_1, v_2_5_2, v_2_6, v_2_7, v_2_8, v_2_9,
        v_4_8b_1, v_4_8b_2, v_3, v_4_1,
        v_obshagi_vpo_combo, v_obshagi_2_5, v_ped_1_2, v_ped_1_3_1_4,
        
        
        # 5. Эталон
        obuch_po_vozrastam, obshagi, ped_kadry, dict_regions, dict_codes,
        
        # 6. Золото - Расчетные колонки
        metric_dolya_obuch_ot_naselenia, metric_dolya_nuzhdaushihsya,
        metric_dolya_ne_v_remonte, metric_dolya_neobespechennyh,
        metric_dolya_trebuushih_remonta, metric_dolya_avariynyh,
        metric_srednyaya_stavka, metric_dolya_nezanyatyh,
        
        # 7. Дашборды
        dashboard_obuch_po_vozrastam, dashboard_obshagi, dashboard_ped_kadry
    ]
)