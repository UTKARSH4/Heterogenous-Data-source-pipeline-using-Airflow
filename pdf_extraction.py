import PyPDF2
import os
import pandas as pd

keywords = ['Deaths','Southwest Border Sectors']
def extract_pdf_data(pdf_path, keywords=keywords):
    start,end = keywords

    with open(pdf_path, 'rb') as file:
        pdf_reader = PyPDF2.PdfReader(file)
        total_pages = len(pdf_reader.pages)
        pg = []
        dflist=[]
        for page_num in range(0, total_pages):


            page = pdf_reader.pages[page_num]
            text = page.extract_text()
            text = text.replace(',', '')      # Remove "\n" characters from the text
            text = text.replace('**** N/A ****', 'N/A')
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
        for v in pg:

            v=v.lstrip()
            v=v.split(" ")
            if len(v)>10:
                diff = len(v)-9
                firstcol = ' '.join(v[0:diff])
                #print(' '.join(v[0:diff]))
                nlist=v[diff:]
                nlist.insert(0,firstcol)
                dflist.append(nlist)
            else :
                firstcol = v[0]
                nlist = v[1:]
                nlist.insert(0, firstcol)
                dflist.append(nlist)
        df=pd.DataFrame(dflist)
        df.columns = ['SECTOR',
                        'Agent Staffing',
                        'Apprehensions',
                        'Other Than Mexican Apprehensions',
                        'Marijuana (pounds)',
                        'Cocaine (pounds)',
                        'Accepted Prosecutions',
                        'Assaults',
                        'Rescues',
                        'Deaths']

    return df


