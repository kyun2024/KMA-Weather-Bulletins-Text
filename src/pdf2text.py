# Applicable only to 2017-2019 weather bulletins

import tabula
from tqdm import tqdm
import os
import argparse
import pandas as pd
from multiprocessing import Pool, Manager

def parse_pdf(pdf_path):
    dfs = tabula.read_pdf(pdf_path, pages='all', pandas_options={'header':None})
    df = dfs[0]
    s = ''
    with pd.option_context("future.no_silent_downcasting", True):
        if(len(df.columns) > 1):
            if(type(df[0][0]) == str and '평년(오늘)' in df[0][0]):
                s = dfs[1].iloc[3][0].replace('\r','\n')
            else:
                try:
                    nalssi_idx = df[1][df[1].str.contains('날씨종합', regex=False).fillna(False).infer_objects(copy=False)].index.tolist()
                except:
                    nalssi_idx = df[0][df[0].str.contains('날씨종합', regex=False).fillna(False).infer_objects(copy=False)].index.tolist()[0]
                    cond0 = (df[0].str.contains('평년', regex=False)).fillna(False).infer_objects(copy=False)
                    cond1 = (df[0].str.contains('어제')).fillna(False).infer_objects(copy=False)
                else:    
                    nalssi_idx = df[1][df[1].str.contains('날씨종합', regex=False).fillna(False).infer_objects(copy=False)].index.tolist()
                    if(len(nalssi_idx) == 0):
                        nalssi_idx = df[0][df[0].str.contains('날씨종합', regex=False).fillna(False).infer_objects(copy=False)].index.tolist()[0]
                    else:
                        nalssi_idx = nalssi_idx[0]
                    cond0 = (df[0].str.contains('평년', regex=False)).fillna(False).infer_objects(copy=False)
                    cond1 = (df[0].str.contains('어제') | df[1].str.contains('어제')).fillna(False).infer_objects(copy=False)
                
                pyung_idx = df[0][cond0&cond1].index.tolist()[0]
                s = df[0][nalssi_idx+1:pyung_idx].dropna().str.cat(sep='\n')
        else:
            nalssi_idx = df[0][df[0].str.contains('날씨종합', regex=False).fillna(False).infer_objects(copy=False)].index.tolist()[0]
            pyung_idx = df[0][(df[0].str.contains('평년', regex=False) & df[0].str.contains('어제')).fillna(False).infer_objects(copy=False)].index.tolist()[0]
            s = df[0][nalssi_idx+1:pyung_idx].dropna().str.cat(sep='\n')
    return s

def write_pdf_text(pdf_str, txt_path):
    try:
        os.makedirs(os.path.dirname(txt_path), exist_ok=True)  # Create directory
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(pdf_str)
    except Exception as e:
        print(f"Failed to save file: {txt_path}, Error: {e}")

# Function to process the PDF files
def process_pdf(pdf_path, pdf_dir, txt_dir, dry_run, progress_queue):
    rel_path = os.path.relpath(pdf_path, pdf_dir)  # Relative path based on pdf_dir
    txt_path = os.path.join(txt_dir, os.path.splitext(rel_path)[0] + ".txt")
    
    if dry_run:
        progress_queue.put(f"{pdf_path} -> {txt_path}")
        return

    os.makedirs(os.path.dirname(txt_path), exist_ok=True)
    pdf_text = parse_pdf(pdf_path)

    if pdf_text == '':
        progress_queue.put(f"An error occurred while processing '{os.path.basename(pdf_path)}'.")
    else:
        write_pdf_text(pdf_text, txt_path)
        progress_queue.put(None)  # Return None if processing is successful

# Function to count all PDF files in the pdf_dir
def count_pdf_files(pdf_dir):
    return sum(len([file for file in files if file.lower().endswith(".pdf")]) 
               for _, _, files in os.walk(pdf_dir))

# Distribute PDF processing tasks using multiprocessing
def main():
    parser = argparse.ArgumentParser(description="Scan PDF directory and convert to TXT")
    parser.add_argument("pdf_dir", type=str, help="Directory containing PDF files")
    parser.add_argument("txt_dir", type=str, help="Directory to save TXT files")
    parser.add_argument("--dry-run", action="store_true", help="Only display paths without performing conversion")
    parser.add_argument("--num-processes", type=int, default=4, help="Number of processes to use")

    args = parser.parse_args()
    pdf_dir = os.path.abspath(args.pdf_dir)
    txt_dir = os.path.abspath(args.txt_dir)
    dry_run = args.dry_run
    num_processes = args.num_processes  # Number of processes for multiprocessing

    # Collect and sort the list of all PDF files
    pdf_files = sorted([os.path.join(root, file) 
                        for root, _, files in os.walk(pdf_dir) 
                        for file in files if file.lower().endswith(".pdf")])
    
    with Manager() as manager:
        progress_queue = manager.Queue()  # Queue to share status between processes
        with tqdm(total=len(pdf_files), desc="Processing PDFs", unit="file") as pbar:
            try:
                with Pool(processes=num_processes) as pool:
                    # Process PDF files using multiprocessing
                    for pdf_path in pdf_files:
                        pool.apply_async(process_pdf, (pdf_path, pdf_dir, txt_dir, dry_run, progress_queue))

                    pool.close()  # Stop accepting new tasks

                    # Update progress bar by fetching results from the queue
                    completed_tasks = 0
                    while completed_tasks < len(pdf_files):
                        result = progress_queue.get()  # Get result from queue
                        if result is not None:
                            print(result)  # Print error messages
                        completed_tasks += 1
                        pbar.update(1)  # Update progress bar

                    pool.join()  # Wait for all processes to finish
            except KeyboardInterrupt:
                print("Process interrupted. Cleaning up...")
                pool.terminate()  # Forcefully terminate subprocesses
                pool.join()  # Wait for all processes to finish

if __name__ == "__main__":
    main()
