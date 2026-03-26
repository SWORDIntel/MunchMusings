import csv
from pathlib import Path
from typing import List, Dict, Any

class LeadTimePredictor:
    """
    Phase 3 Predictive Telemetry: Lead-Time & Projected Shock Algorithm.
    Calculates correlation delay between maritime status and market price shocks.
    """
    
    # Heuristic mapping for Lead-Time (days) and Volatility Factors
    MARITIME_WEIGHTS = {
        "SUEZ": {"lead_time_base": 18, "volatility": 1.2},
        "ASHDOD": {"lead_time_base": 8, "volatility": 1.5},
        "HAIFA": {"lead_time_base": 9, "volatility": 1.4},
        "EASTERN MED": {"lead_time_base": 12, "volatility": 1.1}
    }

    def __init__(self, anomaly_report_path: Path):
        self.anomaly_report_path = Path(anomaly_report_path)

    def _load_anomalies(self) -> List[Dict[str, str]]:
        if not self.anomaly_report_path.exists():
            return []
        try:
            with self.anomaly_report_path.open(newline='', encoding='utf-8') as f:
                return list(csv.DictReader(f))
        except Exception:
            return []

    def get_projections(self) -> List[Dict[str, Any]]:
        anomalies = self._load_anomalies()
        projections = []
        
        for row in anomalies:
            correlation = row.get('Maritime_Correlation', '').upper()
            try:
                impact_score = float(row.get('Impact_Score', 0.5))
            except (ValueError, TypeError):
                impact_score = 0.5
            
            region = row.get('Region', 'Unknown')
            food_raw = row.get('Migration_Indicator_Food', 'STAPLE')
            food = (food_raw or 'STAPLE').split('(')[0].strip().upper()
            
            # Identify which maritime hub is most relevant
            lead_time = 14 # Default
            volatility = 1.0
            
            for hub, weights in self.MARITIME_WEIGHTS.items():
                if hub in correlation:
                    lead_time = weights['lead_time_base']
                    volatility = weights['volatility']
                    break
            
            # Algorithm: Projected Shock = Impact_Score * Volatility * 25 (scaled to %)
            projected_shock = impact_score * volatility * 25
            correlation_delay = max(3, lead_time - (impact_score * 5))
            
            projections.append({
                'region': region,
                'food': food,
                'projected_shock': projected_shock,
                'lead_time': int(correlation_delay),
                'risk_level': "HIGH" if projected_shock > 20 else ("MEDIUM" if projected_shock > 10 else "LOW")
            })
            
        return projections

if __name__ == "__main__":
    # Test
    repo_root = Path(__file__).resolve().parents[1]
    predictor = LeadTimePredictor(repo_root / 'plans/regional_anomaly_report.csv')
    import json
    print(json.dumps(predictor.get_projections(), indent=2))
