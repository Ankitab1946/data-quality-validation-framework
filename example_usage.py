import pandas as pd
from dq_framework.main import DataQualityFramework
from pathlib import Path

def load_sample_data() -> pd.DataFrame:
    """Create a sample dataset for demonstration."""
    return pd.DataFrame({
        'customer_id': ['C001', 'C002', 'C003', 'C002', 'C005'],  # Contains duplicate
        'age': [25, 150, -5, 35, 40],  # Contains out of range values
        'email': [
            'valid@email.com',
            'invalid.email',  # Invalid pattern
            'another@email.com',
            'test@test.com',
            None  # Missing value
        ],
        'country': ['USA', 'INVALID', 'IND', 'CHN', 'LKA'],  # Contains invalid enum
        'column1': [100, 200, 300, 400, 500],
        'column2': [50, 100, 150, 200, 250],
        'column3': [200, 250, 400, 550, 700]  # Business rule violation
    })

def main():
    """Example usage of the Data Quality Validation Framework."""
    try:
        # Initialize the framework
        dq_framework = DataQualityFramework()
        
        # Load sample data
        print("Loading sample data...")
        data = load_sample_data()
        
        # Run validations
        print("\nRunning validations...")
        validation_results = dq_framework.validate_data(data)
        
        # Generate report
        print("\nGenerating validation report...")
        dq_framework.generate_report()
        
        print("\nValidation process completed successfully!")
        print("Check the 'reports' directory for the detailed validation report.")
        
    except Exception as e:
        print(f"\nError: {str(e)}")
        raise

if __name__ == "__main__":
    main()
