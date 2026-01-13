import polars as pl
import os

# ==========================================
# 1. [공통] 데이터 전처리
# ==========================================
def preprocess_lazy_frame(file_path: str) -> pl.LazyFrame:
    try:
        # infer_schema_length=0: 모든 컬럼을 일단 문자로 읽어서 타입 에러 방지
        lf = pl.scan_csv(file_path, infer_schema_length=0)

        # 1. 날짜 및 수치형 변환
        lf = lf.with_columns([
            pl.col("oper_datetime").str.to_datetime(),
            pl.col("fleet_id").cast(pl.String),
            pl.col("car1_value").cast(pl.Float64),
            pl.col("car8_value").cast(pl.Float64),
        ])

        # 2. car1, car8 둘 다 0.02 넘는 로우만 남김
        lf = lf.filter(
            (pl.col("car1_value") > 0.02) &
            (pl.col("car8_value") > 0.02)
        )

        # 3. 데이터 형태 변환
        lf = lf.unpivot(
            index=["oper_datetime", "fleet_id"],  # 고정할 컬럼 (id_vars)
            on=["car1_value", "car8_value"],      # 합칠 컬럼 (value_vars)
            variable_name="car_source",
            value_name="value"
        )

        # 4. car_source 문자열을 숫자(1, 8)로 변환
        lf = lf.with_columns(
            pl.when(pl.col("car_source") == "car1_value")
            .then(1)
            .otherwise(8)
            .cast(pl.Int8)
            .alias("car_no")
        ).drop("car_source")

        return lf

    except Exception as e:
        print(f"전처리 초기화 중 에러: {e}")
        raise e


# ==========================================
# 2. [알고리즘 A] 과전류 및 과부하 검지
# ==========================================
def algo_a_overcurrent(lf: pl.LazyFrame) -> pl.LazyFrame:
    """ 3-1-1. 과전류 (1160 ± 5%) """
    target = 1160
    lower = target * 0.95
    upper = target * 1.05

    return (
        lf.filter(pl.col("value") > 1000)
        .filter(pl.col("value").is_between(lower, upper))
        .select([
            pl.col("oper_datetime"),
            pl.col("fleet_id"),
            pl.col("car_no"),
            pl.lit("과전류 검지").alias("event_no")
        ])
    )

def algo_a_overload(lf: pl.LazyFrame) -> pl.DataFrame:
    """ 3-1-2. 과부하 (547 ± 5%, 1시간 연속) """
    target = 547
    lower = target * 0.95
    upper = target * 1.05

    filtered_lf = lf.filter(
        (pl.col("value") <= 1000) &
        (pl.col("value").is_between(lower, upper))
    )

    try:
        df = filtered_lf.collect(engine="streaming")
    except:
        df = filtered_lf.collect()

    # 빈 데이터일 경우 스키마 정의 (fleet_id는 String으로 통일)
    if df.is_empty():
        return pl.DataFrame(schema={
            "oper_datetime": pl.Datetime,
            "fleet_id": pl.String,      # <--- [확인] 여기도 String
            "car_no": pl.Int8,
            "event_no": pl.String
        })

    df = df.sort(["fleet_id", "car_no", "oper_datetime"])

    # 1시간 연속성 체크 (Gap: 10분)
    gap_threshold_seconds = 600

    df = df.with_columns([
        (pl.col("oper_datetime").diff().dt.total_seconds().fill_null(0) > gap_threshold_seconds)
        .over(["fleet_id", "car_no"])
        .cum_sum()
        .alias("session_id")
    ])

    result = (
        df.group_by(["fleet_id", "car_no", "session_id"])
        .agg([
            pl.col("oper_datetime").min().alias("start_time"),
            pl.col("oper_datetime").max().alias("end_time"),
            pl.col("oper_datetime").count().alias("cnt")
        ])
        .with_columns(
            (pl.col("end_time") - pl.col("start_time")).dt.total_seconds().alias("duration")
        )
        .filter(pl.col("duration") >= 3600)
    )

    return result.select([
        pl.col("start_time").alias("oper_datetime"),
        pl.col("fleet_id"),
        pl.col("car_no"),
        pl.lit("과부하 검지").alias("event_no")
    ])


# ==========================================
# 3. [알고리즘 B] 이상 전류 검지 (통계)
# ==========================================
def algo_b_anomaly(lf: pl.LazyFrame) -> pl.LazyFrame:
    """ 4-1. 통계적 이상치 (|값 - 평균| / 평균 > 0.25) """
    return (
        lf.with_columns([
            pl.col("value").sum().over(["fleet_id", "car_no"]).alias("grp_sum"),
            pl.col("value").count().over(["fleet_id", "car_no"]).alias("grp_cnt")
        ])
        .with_columns(
            ((pl.col("grp_sum") - pl.col("value")) / (pl.col("grp_cnt") - 1)).alias("loo_mean")
        )
        .filter(pl.col("loo_mean").is_not_null() & (pl.col("loo_mean") != 0))
        .filter(
            ((pl.col("value") - pl.col("loo_mean")).abs() / pl.col("loo_mean")) > 0.25
        )
        .select([
            pl.col("oper_datetime"),
            pl.col("fleet_id"),
            pl.col("car_no"),
            pl.lit("이상 전류 검지").alias("event_no")
        ])
    )


# ==========================================
# 4. 실행부 (Main)
# ==========================================
if __name__ == "__main__":
    csv_file = "siv_Inverter.csv"

    if not os.path.exists(csv_file):
        print(f"오류: '{csv_file}' 파일을 찾을 수 없습니다.")
        exit()

    print(f">>> '{csv_file}' 데이터 처리 시작...")

    base_lf = preprocess_lazy_frame(csv_file)

    print("- 알고리즘 A-1 (과전류) 분석 중...")
    res_a1 = algo_a_overcurrent(base_lf).collect(engine="streaming")

    print("- 알고리즘 A-2 (과부하-시간연속) 분석 중...")
    res_a2 = algo_a_overload(base_lf)

    print("- 알고리즘 B (이상전류-통계) 분석 중...")
    res_b = algo_b_anomaly(base_lf).collect(engine="streaming")

    # 결과 합치기
    final_df = pl.concat([res_a1, res_a2, res_b])

    # ---------------------------------------------------------
    # [수정] 미리보기 대신 알고리즘별 건수 요약 출력
    # ---------------------------------------------------------
    count_a1 = len(res_a1)
    count_a2 = len(res_a2)
    count_b = len(res_b)
    total_count = len(final_df)

    print("\n" + "="*40)
    print("       📊 데이터 분석 결과 요약")
    print("="*40)
    print(f" 1. 알고리즘 A-1 (과전류)   : {count_a1:>5} 건")
    print(f" 2. 알고리즘 A-2 (과부하)   : {count_a2:>5} 건")
    print(f" 3. 알고리즘 B   (이상전류) : {count_b:>5} 건")
    print("-" * 40)
    print(f"    총 이벤트 발생 건수     : {total_count:>5} 건")
    print("="*40)

    # 데이터가 있을 경우에만 파일 저장
    if not final_df.is_empty():
        # 날짜순 정렬 및 문자열 변환
        final_df = final_df.sort("oper_datetime")

        result_list = final_df.with_columns(
            pl.col("oper_datetime").dt.to_string("%Y-%m-%d %H:%M:%S")
        ).to_dicts()

        output_file = "result_events.txt"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(str(result_list))

        print(f"\n>>> 상세 데이터가 '{output_file}' 파일에 저장되었습니다.")
    else:
        print("\n>>> 조건에 맞는 이벤트가 하나도 검출되지 않았습니다.")