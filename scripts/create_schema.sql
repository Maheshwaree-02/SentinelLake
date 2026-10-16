CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    step INT,
    type VARCHAR(20),
    amount DECIMAL(18, 2),
    name_orig VARCHAR(50),
    oldbalance_org DECIMAL(18, 2),
    newbalance_orig DECIMAL(18, 2),
    name_dest VARCHAR(50),
    oldbalance_dest DECIMAL(18, 2),
    newbalance_dest DECIMAL(18, 2),
    is_fraud SMALLINT,
    is_flagged_fraud SMALLINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fraud_scores (
    transaction_id VARCHAR(50) PRIMARY KEY,
    risk_score DECIMAL(5, 4),
    model_version VARCHAR(20),
    scored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS recommendations (
    transaction_id VARCHAR(50) PRIMARY KEY,
    recommended_action VARCHAR(30),
    confidence DECIMAL(5, 4),
    reason TEXT,
    ranked_scores JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);