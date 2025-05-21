import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import pandas as pd
import great_expectations as ge
from great_expectations.dataset import PandasDataset

class DataValidator:
    def __init__(self, config: Dict):
        """Initialize the Data Validator."""
        self.config = config
        self.logger = logging.getLogger('dq_framework.validations')
        self.validation_results = {}

    def validate_dataset(self, data: pd.DataFrame, source_name: str) -> Dict:
        """Run all configured validations on the dataset."""
        try:
            # Convert to Great Expectations dataset
            ge_dataset = ge.from_pandas(data)
            
            self.validation_results = {
                'source_name': source_name,
                'timestamp': datetime.now().isoformat(),
                'validations': {}
            }

            # Run all configured validations
            self._run_count_validation(ge_dataset)
            self._run_checksum_validation(ge_dataset)
            self._run_duplicate_check(ge_dataset)
            self._run_pattern_check(ge_dataset)
            self._run_enumeration_check(ge_dataset)
            self._run_mandatory_check(ge_dataset)
            self._run_range_check(ge_dataset)
            self._run_type_check(ge_dataset)
            self._run_unique_check(ge_dataset)
            self._run_business_rules(ge_dataset)

            return self.validation_results

        except Exception as e:
            self.logger.error(f"Error validating dataset: {str(e)}")
            raise

    def _run_count_validation(self, dataset: PandasDataset) -> None:
        """Validate record count against expected value."""
        validation_type = 'count_validation'
        self.logger.info(f"Running {validation_type}")
        
        try:
            # Get expected count from configuration
            expected_count = self.config.get('expected_count', 0)
            
            # Validate count
            actual_count = len(dataset)
            result = {
                'status': 'completed' if actual_count == expected_count else 'failed',
                'details': {
                    'expected_count': expected_count,
                    'actual_count': actual_count,
                    'difference': actual_count - expected_count
                }
            }
            
            self.validation_results['validations'][validation_type] = result
            
        except Exception as e:
            self.logger.error(f"Error in {validation_type}: {str(e)}")
            self._log_validation_error(validation_type, str(e))

    def _run_checksum_validation(self, dataset: PandasDataset) -> None:
        """Validate checksum of specified columns."""
        validation_type = 'checksum_validation'
        self.logger.info(f"Running {validation_type}")
        
        try:
            checksum_columns = self.config.get('checksum_columns', [])
            checksums = {}
            
            for column in checksum_columns:
                if column in dataset.columns:
                    checksums[column] = dataset[column].sum()
            
            self.validation_results['validations'][validation_type] = {
                'status': 'completed',
                'details': {'checksums': checksums}
            }
            
        except Exception as e:
            self.logger.error(f"Error in {validation_type}: {str(e)}")
            self._log_validation_error(validation_type, str(e))

    def _run_duplicate_check(self, dataset: PandasDataset) -> None:
        """Check for duplicate records."""
        validation_type = 'duplicate_check'
        self.logger.info(f"Running {validation_type}")
        
        try:
            # Get columns to check for duplicates
            check_columns = self.config.get('duplicate_check_columns', dataset.columns.tolist())
            
            # Find duplicates
            duplicates = dataset.duplicated(subset=check_columns, keep='first')
            duplicate_count = duplicates.sum()
            
            # Get sample of duplicate records
            duplicate_samples = dataset[duplicates].head(5).to_dict('records')
            
            result = {
                'status': 'completed' if duplicate_count == 0 else 'failed',
                'details': {
                    'duplicate_count': int(duplicate_count),
                    'checked_columns': check_columns,
                    'duplicate_samples': duplicate_samples
                }
            }
            
            self.validation_results['validations'][validation_type] = result
            
        except Exception as e:
            self.logger.error(f"Error in {validation_type}: {str(e)}")
            self._log_validation_error(validation_type, str(e))

    def _run_pattern_check(self, dataset: PandasDataset) -> None:
        """Check if values match specified patterns."""
        validation_type = 'pattern_check'
        self.logger.info(f"Running {validation_type}")
        
        try:
            pattern_rules = self.config.get('pattern', {})
            results = {}
            
            for column, pattern in pattern_rules.items():
                if column in dataset.columns:
                    # Use Great Expectations to check pattern
                    validation = dataset.expect_column_values_to_match_regex(
                        column,
                        pattern
                    )
                    results[column] = {
                        'success': validation.success,
                        'unexpected_count': validation.result['unexpected_count'],
                        'unexpected_samples': validation.result['unexpected_list'][:5]
                    }
            
            self.validation_results['validations'][validation_type] = {
                'status': 'completed' if all(r['success'] for r in results.values()) else 'failed',
                'details': results
            }
            
        except Exception as e:
            self.logger.error(f"Error in {validation_type}: {str(e)}")
            self._log_validation_error(validation_type, str(e))

    def _run_enumeration_check(self, dataset: PandasDataset) -> None:
        """Check if values are within allowed enumerations."""
        validation_type = 'enumeration_check'
        self.logger.info(f"Running {validation_type}")
        
        try:
            enum_rules = self.config.get('enumerations', {})
            results = {}
            
            for column, allowed_values in enum_rules.items():
                if column in dataset.columns:
                    validation = dataset.expect_column_values_to_be_in_set(
                        column,
                        allowed_values
                    )
                    results[column] = {
                        'success': validation.success,
                        'unexpected_count': validation.result['unexpected_count'],
                        'unexpected_samples': validation.result['unexpected_list'][:5]
                    }
            
            self.validation_results['validations'][validation_type] = {
                'status': 'completed' if all(r['success'] for r in results.values()) else 'failed',
                'details': results
            }
            
        except Exception as e:
            self.logger.error(f"Error in {validation_type}: {str(e)}")
            self._log_validation_error(validation_type, str(e))

    def _run_mandatory_check(self, dataset: PandasDataset) -> None:
        """Check for null values in mandatory fields."""
        validation_type = 'mandatory_check'
        self.logger.info(f"Running {validation_type}")
        
        try:
            mandatory_fields = [
                field['column_name'] 
                for field in self.config.get('template', []) 
                if field.get('mandatory', False)
            ]
            
            results = {}
            for field in mandatory_fields:
                if field in dataset.columns:
                    validation = dataset.expect_column_values_to_not_be_null(field)
                    results[field] = {
                        'success': validation.success,
                        'null_count': validation.result['unexpected_count'],
                        'null_examples': validation.result['unexpected_list'][:5]
                    }
            
            self.validation_results['validations'][validation_type] = {
                'status': 'completed' if all(r['success'] for r in results.values()) else 'failed',
                'details': results
            }
            
        except Exception as e:
            self.logger.error(f"Error in {validation_type}: {str(e)}")
            self._log_validation_error(validation_type, str(e))

    def _run_range_check(self, dataset: PandasDataset) -> None:
        """Check if numeric values are within specified ranges."""
        validation_type = 'range_check'
        self.logger.info(f"Running {validation_type}")
        
        try:
            range_rules = [
                field for field in self.config.get('template', [])
                if 'range' in field
            ]
            
            results = {}
            for rule in range_rules:
                column = rule['column_name']
                if column in dataset.columns:
                    range_config = rule['range']
                    validation = dataset.expect_column_values_to_be_between(
                        column,
                        min_value=range_config['bottom'],
                        max_value=range_config['top'],
                        include_min=range_config.get('scope') == 'inclusive',
                        include_max=range_config.get('scope') == 'inclusive'
                    )
                    results[column] = {
                        'success': validation.success,
                        'unexpected_count': validation.result['unexpected_count'],
                        'unexpected_samples': validation.result['unexpected_list'][:5]
                    }
            
            self.validation_results['validations'][validation_type] = {
                'status': 'completed' if all(r['success'] for r in results.values()) else 'failed',
                'details': results
            }
            
        except Exception as e:
            self.logger.error(f"Error in {validation_type}: {str(e)}")
            self._log_validation_error(validation_type, str(e))

    def _run_type_check(self, dataset: PandasDataset) -> None:
        """Check if columns have the correct data type."""
        validation_type = 'type_check'
        self.logger.info(f"Running {validation_type}")
        
        try:
            type_rules = [
                field for field in self.config.get('template', [])
                if 'type_name' in field
            ]
            
            results = {}
            for rule in type_rules:
                column = rule['column_name']
                if column in dataset.columns:
                    expected_type = rule['type_name']
                    # Map type names to pandas dtypes
                    type_mapping = {
                        'string': 'object',
                        'integer': 'int64',
                        'float': 'float64',
                        'boolean': 'bool',
                        'date': 'datetime64[ns]'
                    }
                    
                    actual_type = str(dataset[column].dtype)
                    expected_pandas_type = type_mapping.get(expected_type.lower(), expected_type)
                    
                    results[column] = {
                        'success': actual_type == expected_pandas_type,
                        'expected_type': expected_pandas_type,
                        'actual_type': actual_type
                    }
            
            self.validation_results['validations'][validation_type] = {
                'status': 'completed' if all(r['success'] for r in results.values()) else 'failed',
                'details': results
            }
            
        except Exception as e:
            self.logger.error(f"Error in {validation_type}: {str(e)}")
            self._log_validation_error(validation_type, str(e))

    def _run_unique_check(self, dataset: PandasDataset) -> None:
        """Check if specified columns have unique values."""
        validation_type = 'unique_check'
        self.logger.info(f"Running {validation_type}")
        
        try:
            unique_fields = [
                field['column_name'] 
                for field in self.config.get('template', []) 
                if field.get('unique', False)
            ]
            
            results = {}
            for field in unique_fields:
                if field in dataset.columns:
                    validation = dataset.expect_column_values_to_be_unique(field)
                    results[field] = {
                        'success': validation.success,
                        'duplicate_count': validation.result['unexpected_count'],
                        'duplicate_examples': validation.result['unexpected_list'][:5]
                    }
            
            self.validation_results['validations'][validation_type] = {
                'status': 'completed' if all(r['success'] for r in results.values()) else 'failed',
                'details': results
            }
            
        except Exception as e:
            self.logger.error(f"Error in {validation_type}: {str(e)}")
            self._log_validation_error(validation_type, str(e))

    def _run_business_rules(self, dataset: PandasDataset) -> None:
        """Run custom business rule validations."""
        validation_type = 'business_rules'
        self.logger.info(f"Running {validation_type}")
        
        try:
            business_rules = self.config.get('business_rules', [])
            results = {}
            
            for rule in business_rules:
                rule_id = rule['rule_id']
                # Create a query string for the business rule
                query = f"({rule['condition']}) & ({rule['validation']})"
                
                # Apply the rule and get violations
                try:
                    mask = dataset.query(query)
                    violations = dataset[~mask]
                    
                    results[rule_id] = {
                        'success': len(violations) == 0,
                        'violation_count': len(violations),
                        'violation_samples': violations.head(5).to_dict('records')
                    }
                except Exception as rule_error:
                    results[rule_id] = {
                        'success': False,
                        'error': str(rule_error)
                    }
            
            self.validation_results['validations'][validation_type] = {
                'status': 'completed' if all(r['success'] for r in results.values()) else 'failed',
                'details': results
            }
            
        except Exception as e:
            self.logger.error(f"Error in {validation_type}: {str(e)}")
            self._log_validation_error(validation_type, str(e))

    def _log_validation_error(self, validation_type: str, error_message: str) -> None:
        """Log validation error and update validation results."""
        self.validation_results['validations'][validation_type] = {
            'status': 'error',
            'error': error_message
        }
