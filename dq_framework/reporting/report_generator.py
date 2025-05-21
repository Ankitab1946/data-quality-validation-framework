import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path
import plotly.graph_objects as go
import plotly.express as px
from jinja2 import Environment, FileSystemLoader
import pandas as pd
import weasyprint

class ReportGenerator:
    def __init__(self, config: Dict):
        """Initialize the Report Generator."""
        self.config = config
        self.logger = logging.getLogger('dq_framework.reporting')
        self.template_dir = Path(__file__).parent / 'templates'
        self.env = Environment(loader=FileSystemLoader(str(self.template_dir)))
        
    def generate_report(self, validation_results: Dict, output_format: List[str] = None) -> Dict[str, str]:
        """Generate validation report in specified formats."""
        if output_format is None:
            output_format = self.config['reporting']['format']
            
        report_files = {}
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            # Generate report content
            report_data = self._prepare_report_data(validation_results)
            
            # Generate reports in specified formats
            for fmt in output_format:
                if fmt.lower() == 'html':
                    report_files['html'] = self._generate_html_report(report_data, timestamp)
                elif fmt.lower() == 'pdf':
                    report_files['pdf'] = self._generate_pdf_report(report_data, timestamp)
                elif fmt.lower() == 'excel':
                    report_files['excel'] = self._generate_excel_report(report_data, timestamp)
                    
            return report_files
            
        except Exception as e:
            self.logger.error(f"Error generating report: {str(e)}")
            raise

    def _prepare_report_data(self, validation_results: Dict) -> Dict:
        """Prepare data for report generation."""
        return {
            'summary': self._generate_summary(validation_results),
            'detailed_results': self._prepare_detailed_results(validation_results),
            'visualizations': self._generate_visualizations(validation_results),
            'timestamp': datetime.now().isoformat(),
            'branding': self.config['reporting']['branding']
        }

    def _generate_summary(self, validation_results: Dict) -> Dict:
        """Generate executive summary of validation results."""
        total_validations = len(validation_results)
        passed_validations = sum(1 for result in validation_results.values() 
                               if result.get('status') == 'completed')
        
        return {
            'total_validations': total_validations,
            'passed_validations': passed_validations,
            'failed_validations': total_validations - passed_validations,
            'success_rate': (passed_validations / total_validations * 100) if total_validations > 0 else 0
        }

    def _prepare_detailed_results(self, validation_results: Dict) -> List[Dict]:
        """Prepare detailed validation results for reporting."""
        detailed_results = []
        
        for validation_type, result in validation_results.items():
            detailed_results.append({
                'validation_type': validation_type,
                'status': result.get('status'),
                'timestamp': result.get('timestamp'),
                'details': result.get('details', {}),
                'error_samples': self._get_error_samples(result)
            })
            
        return detailed_results

    def _generate_visualizations(self, validation_results: Dict) -> Dict:
        """Generate visualization data for the report."""
        # Create validation status pie chart
        status_counts = {'Passed': 0, 'Failed': 0}
        for result in validation_results.values():
            if result.get('status') == 'completed':
                status_counts['Passed'] += 1
            else:
                status_counts['Failed'] += 1

        status_pie = go.Figure(data=[go.Pie(
            labels=list(status_counts.keys()),
            values=list(status_counts.values()),
            marker_colors=['#28a745', '#dc3545']
        )])
        
        # Create validation type bar chart
        validation_types = {}
        for v_type, result in validation_results.items():
            validation_types[v_type] = len(result.get('details', {}))
            
        type_bar = go.Figure(data=[go.Bar(
            x=list(validation_types.keys()),
            y=list(validation_types.values())
        )])
        
        return {
            'status_pie': status_pie.to_html(full_html=False),
            'type_bar': type_bar.to_html(full_html=False)
        }

    def _generate_html_report(self, report_data: Dict, timestamp: str) -> str:
        """Generate HTML report."""
        template = self.env.get_template('report_template.html')
        output_path = f'reports/validation_report_{timestamp}.html'
        
        # Ensure reports directory exists
        os.makedirs('reports', exist_ok=True)
        
        # Generate HTML report
        html_content = template.render(**report_data)
        with open(output_path, 'w') as f:
            f.write(html_content)
            
        return output_path

    def _generate_pdf_report(self, report_data: Dict, timestamp: str) -> str:
        """Generate PDF report."""
        html_path = self._generate_html_report(report_data, timestamp)
        pdf_path = f'reports/validation_report_{timestamp}.pdf'
        
        # Convert HTML to PDF
        weasyprint.HTML(filename=html_path).write_pdf(pdf_path)
        return pdf_path

    def _generate_excel_report(self, report_data: Dict, timestamp: str) -> str:
        """Generate Excel report with multiple sheets."""
        excel_path = f'reports/validation_report_{timestamp}.xlsx'
        
        # Create Excel writer
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            # Summary sheet
            summary_df = pd.DataFrame([report_data['summary']])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Detailed results sheet
            detailed_df = pd.DataFrame(report_data['detailed_results'])
            detailed_df.to_excel(writer, sheet_name='Detailed Results', index=False)
            
        return excel_path

    def _get_error_samples(self, result: Dict, max_samples: int = 5) -> List[Dict]:
        """Get sample of error records from validation result."""
        error_samples = result.get('error_samples', [])
        return error_samples[:max_samples]

    def email_report(self, report_files: Dict[str, str]) -> None:
        """Email the generated reports to configured recipients."""
        # Email functionality to be implemented
        pass

    def _create_report_directory(self) -> None:
        """Create reports directory if it doesn't exist."""
        os.makedirs('reports', exist_ok=True)
