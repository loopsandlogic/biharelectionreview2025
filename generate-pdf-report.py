import polars as pl
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, Spacer, Paragraph
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER



def generate_pdf_report(record: dict) -> None:
    # Create a PDF document
    pdf_file_name = f"vote_report_{record.get('constituency_no')}_{record.get('serial_num')}.pdf"
    doc = SimpleDocTemplate(pdf_file_name, pagesize=A4)
    elements = []

    title = f"<para align='center'><font size='18'>Assembly Constituency </font><font color='blue' size='18'>{record.get('constituency_no')} - {record.get('constituency').upper()} <b>(Bihar)</b></font></para>"
    elements.append(Paragraph(title))


    # Build the PDF document
    doc.build(elements)

def main():
    # Load the data from a CSV file
    df = pl.read_csv('bihar_election_results_2025.csv')
    df_top5 = df.head(1)

    for record in df_top5.to_dicts():
        generate_pdf_report(record)

if __name__ == "__main__":
    main()