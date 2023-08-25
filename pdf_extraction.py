import PyPDF2
import os
import pandas as pd
import re
def search_pdf_for_word(pdf_path, start, end):
    matching_paragraphs = []

    with open(pdf_path, 'rb') as file:
        pdf_reader = PyPDF2.PdfReader(file)
        total_pages = len(pdf_reader.pages)
        pg = []
        dflist=[]
        for page_num in range(0, total_pages):


            page = pdf_reader.pages[page_num]
            text = page.extract_text()
            text = text.replace(',', '')      # Remove "\n" characters from the text

            paragraphs = text.split('\n')
            #x=paragraphs.find(end)
            pg.extend(paragraphs)
            for v in pg:
                if start in v:
                    first=pg.index(v)+1
                    break;
                else:
                    first=0
            for v in pg:
                if end in v:
                    last=pg.index(v)
                    break;
                else:
                    last=len(pg)

            pg=pg[first:last]
            del pg[3]
            del pg[12]
            for v in pg:

                dflist.append(v.strip().split(" "))
            df=pd.DataFrame(dflist)


    return df



pdf_path = "C:/Users/utkarsh.verma/Downloads/usbp_stats_fy2017_sector_profile.pdf"
keywords = ['Key Items:']

print(search_pdf_for_word(pdf_path,'Deaths' , 'Southwest Border Sectors'))
