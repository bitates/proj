import sqlite3
import os
import re

DB_TV_SHOWS = r"C:\Users\guilh\Documents\PjBd\Proj\tv_shows.db"
DB_COMP = r"C:\Users\guilh\Documents\PjBd\Proj\TVShows_Companies.db"

def clean_company(n):
    if n is None:
        return None
    n = n.strip()
    n = re.sub(r'\s+', ' ', n)  # remover espaços duplos
    n = n.strip(",; ")
    return n

def main():
    if not os.path.exists(DB_TV_SHOWS):
        raise FileNotFoundError("tv_shows.db não encontrado.")
    if not os.path.exists(DB_COMP):
        raise FileNotFoundError("TVShows_Companies.db não encontrado.")

    # conectar
    conn_tv = sqlite3.connect(DB_TV_SHOWS)
    conn_comp = sqlite3.connect(DB_COMP)

    cur_tv = conn_tv.cursor()
    cur_comp = conn_comp.cursor()

    # garantir que o campo name é único
    cur_comp.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_companies_name
        ON COMPANIES(name COLLATE NOCASE);
    """)
    conn_comp.commit()

    # buscar todas as linhas de production_companies
    cur_tv.execute("SELECT production_companies FROM tv_shows")
    rows = cur_tv.fetchall()

    empresas_unicas = set()

    for (raw,) in rows:
        if raw is None:
            continue

        partes = raw.split(",")

        for empresa in partes:
            empresa = clean_company(empresa)
            if empresa:  
                empresas_unicas.add(empresa)

    # inserir na COMPANIES
    total = 0
    for empresa in empresas_unicas:
        cur_comp.execute("""
            INSERT OR IGNORE INTO COMPANIES (name)
            VALUES (?);
        """, (empresa,))
        total += 1

    conn_comp.commit()
    conn_tv.close()
    conn_comp.close()

    print(f"Processo concluído! {len(empresas_unicas)} empresas encontradas.")
    print("Inserções tentadas:", total)

main()
