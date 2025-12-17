import html2text
from bs4 import BeautifulSoup

# --- Configuration ---
html_file_path = '/Users/swastik/Documents/Pathway Project/parallel_241025/Financial Data/data/filings/MSFT/0000789019-25-000059.html' # Your SEC HTML file
output_text_file = 'output_chunks_v2.txt'
# ---------------------

# Create an html2text converter
h_converter = html2text.HTML2Text()
h_converter.ignore_links = True
h_converter.ignore_images = True
h_converter.body_width = 0 # Don't wrap lines

def get_clean_text_chunk(soup_element):
    """Converts a BeautifulSoup element (like a table) to clean text."""
    if not soup_element:
        return None
    
    # Convert the element back to a string
    html_string = str(soup_element)
    
    # Use html2text to get clean, readable text
    text_chunk = h_converter.handle(html_string)
    
    # Clean up excess newlines
    return '\n'.join(line for line in text_chunk.splitlines() if line.strip())

# --- Main execution ---
all_text_chunks = []

try:
    with open(html_file_path, 'r', encoding='utf-8') as f:
        html_content = f.read()

    soup = BeautifulSoup(html_content, 'html.parser')

    # --- Chunk 1: Filing Metadata (NEW, ROBUST LOGIC) ---
    # Find the text "Reporting Person" and get its parent table
    info_header = soup.find(string=lambda t: t and "Name and Address of Reporting Person" in t)
    if info_header:
        info_table = info_header.find_parent('table')
        info_chunk = get_clean_text_chunk(info_table)
        if info_chunk:
            all_text_chunks.append("--- FILING METADATA ---\n" + info_chunk)
    else:
        print("WARNING: Could not find 'Reporting Person' metadata table.")


    # --- Chunk 2: Table I ---
    table_i_header_tag = soup.find('b', string=lambda t: t and t.strip().startswith('Table I'))
    if table_i_header_tag:
        table_i = table_i_header_tag.find_parent('table')
        table_i_chunk = get_clean_text_chunk(table_i)
        if table_i_chunk:
            all_text_chunks.append(table_i_chunk)
    else:
        print("WARNING: Could not find 'Table I'.")


    # --- Chunk 3: Table II ---
    table_ii_header_tag = soup.find('b', string=lambda t: t and t.strip().startswith('Table II'))
    if table_ii_header_tag:
        table_ii = table_ii_header_tag.find_parent('table')
        table_ii_chunk = get_clean_text_chunk(table_ii)
        if table_ii_chunk:
            all_text_chunks.append(table_ii_chunk)
    else:
        print("WARNING: Could not find 'Table II'.")

            
    # --- Chunk 4: Footnotes / Explanation ---
    explanation_header = soup.find('b', string=lambda t: t and t.strip() == 'Explanation of Responses:')
    if explanation_header:
        explanation_table = explanation_header.find_parent('table')
        explanation_chunk = get_clean_text_chunk(explanation_table)
        if explanation_chunk:
            all_text_chunks.append(explanation_chunk)
    else:
        print("WARNING: Could not find 'Explanation of Responses'.")
        

    # --- Chunk 5: Signature (NEW) ---
    # Find the text "** Signature of Reporting Person" and get its parent table
    sig_header = soup.find(string=lambda t: t and "** Signature of Reporting Person" in t)
    if sig_header:
        sig_table = sig_header.find_parent('table')
        sig_chunk = get_clean_text_chunk(sig_table)
        if sig_chunk:
            all_text_chunks.append("--- SIGNATURES ---\n" + sig_chunk)
    else:
        print("WARNING: Could not find 'Signature' table.")


    # --- Save all chunks to a single file for review ---
    with open(output_text_file, 'w', encoding='utf-8') as f:
        print(f"\nSuccessfully extracted {len(all_text_chunks)} text chunks to '{output_text_file}'")
        for i, chunk in enumerate(all_text_chunks):
            f.write(f"--- CHUNK {i+1} ---\n")
            f.write(chunk)
            f.write("\n\n" + "="*80 + "\n\n")

except FileNotFoundError:
    print(f"ERROR: The file '{html_file_path}' was not found.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")