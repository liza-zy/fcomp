import duckdb

con = duckdb.connect("data_lake/rl_training.duckdb")

sql = """
UPDATE risk_profiles
SET
    max_bond_weight = NULL,
    max_fx_weight = CASE profile_name
        WHEN 'Ultra-Conservative' THEN 0.15
        WHEN 'Conservative' THEN 0.20
        WHEN 'Balanced' THEN 0.25
        WHEN 'Growth' THEN 0.20
        WHEN 'Aggressive' THEN 0.15
        ELSE max_fx_weight
    END,
    max_metal_weight = CASE profile_name
        WHEN 'Ultra-Conservative' THEN 0.20
        WHEN 'Conservative' THEN 0.25
        WHEN 'Balanced' THEN 0.30
        WHEN 'Growth' THEN 0.20
        WHEN 'Aggressive' THEN 0.15
        ELSE max_metal_weight
    END,
    risk_penalty_lambda = CASE profile_name
        WHEN 'Ultra-Conservative' THEN 8.0
        WHEN 'Conservative' THEN 6.0
        WHEN 'Balanced' THEN 4.0
        WHEN 'Growth' THEN 2.5
        WHEN 'Aggressive' THEN 1.5
        ELSE risk_penalty_lambda
    END,
    turnover_penalty_lambda = CASE profile_name
        WHEN 'Ultra-Conservative' THEN 2.0
        WHEN 'Conservative' THEN 1.5
        WHEN 'Balanced' THEN 1.0
        WHEN 'Growth' THEN 0.75
        WHEN 'Aggressive' THEN 0.5
        ELSE turnover_penalty_lambda
    END
"""

con.execute(sql)

print("UPDATED OK")

for row in con.execute("SELECT * FROM risk_profiles ORDER BY risk_profile_id").fetchall():
    print(row)

con.close()
