import sqlite3
import os
import re

DB_TV_SHOWS = r"C:\Users\guilh\Documents\PjBd\Proj\tv_shows.db"
DB_COMP = r"C:\Users\guilh\Documents\PjBd\Proj\TVShows_Companies.db"

def clean_company(name):
    if not name:
        return None
    name = name.strip()
    name = re.sub(r'\s+', ' ', name)
    return name.strip(",; ")

def main():
    if not os.path.exists(DB_TV_SHOWS):
        raise FileNotFoundError("tv_shows.db não encontrado.")
    if not os.path.exists(DB_COMP):
        raise FileNotFoundError("TVShows_Companies.db não encontrado.")

    conn_tv = sqlite3.connect(DB_TV_SHOWS)
    cur_tv = conn_tv.cursor()

    conn_comp = sqlite3.connect(DB_COMP)
    cur_comp = conn_comp.cursor()

    # Garantir que a tabela existe
    cur_comp.execute("""
        CREATE TABLE IF NOT EXISTS SERIES_COMPANIES (
            id INTEGER NOT NULL,
            companie_id INTEGER NOT NULL,
            PRIMARY KEY (id, companie_id),
            FOREIGN KEY (companie_id) REFERENCES COMPANIES(companie_id)
        );
    """)
    conn_comp.commit()

    # Buscar séries e empresas
    cur_tv.execute("SELECT id, production_companies FROM tv_shows;")
    shows = cur_tv.fetchall()

    relacoes = 0

    for show_id, raw_companies in shows:
        if not raw_companies:
            continue

        # separar por vírgula
        lista = raw_companies.split(",")

        for empresa in lista:
            empresa = clean_company(empresa)
            if not empresa:
                continue

            # encontrar o companie_id correspondente
            cur_comp.execute("""
                SELECT companie_id FROM COMPANIES
                WHERE name = ?
                COLLATE NOCASE;
            """, (empresa,))
            result = cur_comp.fetchone()

            if result:
                companie_id = result[0]

                # inserir na tabela relacional
                cur_comp.execute("""
                    INSERT OR IGNORE INTO SERIES_COMPANIES (id, companie_id)
                    VALUES (?, ?);
                """, (show_id, companie_id))

                relacoes += 1

    conn_comp.commit()
    conn_tv.close()
    conn_comp.close()

    print(f"Relações inseridas na tabela SERIES_COMPANIES: {relacoes}")

main()
