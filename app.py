import pandas as pd
from flask import Flask, render_template, request, send_file, redirect, url_for, flash
import re
import csv
import io

app = Flask(__name__)
app.secret_key = "chave-simples-para-flash"

COLUNAS_PADRAO = [
    "Nome completo",
    "Data de nascimento",
    "CPF",
    "E-mail",
    "Carga horária do certificado",
    "Início da participação",
    "Término da participação"
]

COLUNAS_DATA = [
    "Data de nascimento",
    "Início da participação",
    "Término da participação"
]

MAPA_COLUNAS = {
    "Nome completo": [
        "nome completo", "nome", "participante", "nome do participante", "nome do aluno"
    ],
    "Data de nascimento": [
        "data de nascimento", "nascimento", "dt nascimento", "dt de nascimento", "data nasc"
    ],
    "CPF": [
        "cpf", "cpf do participante"
    ],
    "E-mail": [
        "e-mail", "email", "correio eletronico", "mail"
    ],
    "Carga horária do certificado": [
        "carga horaria do certificado", "carga horária do certificado",
        "carga horaria", "carga horária", "carga", "ch", "horas"
    ],
    "Início da participação": [
        "inicio da participacao", "início da participação", "inicio", "início",
        "data de inicio", "data de início", "data inicial", "comeco", "começo",
        "data inicio"
    ],
    "Término da participação": [
        "termino da participacao", "término da participação", "termino", "término",
        "data de fim", "data final", "fim", "data termino", "data término"
    ]
}


def normalizar_texto(texto):
    if pd.isna(texto):
        return ""
    texto = str(texto).strip().lower()
    substituicoes = {
        "á": "a", "à": "a", "ã": "a", "â": "a",
        "é": "e", "ê": "e",
        "í": "i",
        "ó": "o", "ô": "o", "õ": "o",
        "ú": "u",
        "ç": "c"
    }
    for antigo, novo in substituicoes.items():
        texto = texto.replace(antigo, novo)
    texto = re.sub(r"\s+", " ", texto)
    return texto


def parse_datas_serie(serie: pd.Series) -> pd.Series:
    s = serie.astype(str).str.strip()
    s = s.replace({
        "": None, "nan": None, "NaN": None, "None": None,
        "nat": None, "NaT": None
    })

    try:
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True, format="mixed")
    except TypeError:
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)

    return dt


def _converter_datas_para_ddmmaa(serie: pd.Series) -> pd.Series:
    dt = parse_datas_serie(serie)
    return dt.dt.strftime("%d/%m/%Y").fillna("")


def eh_cabecalho(linha) -> bool:
    valores = [normalizar_texto(v) for v in linha.tolist()]
    acertos = 0

    for valor in valores:
        for lista_sinonimos in MAPA_COLUNAS.values():
            sinonimos_normalizados = [normalizar_texto(s) for s in lista_sinonimos]
            if valor in sinonimos_normalizados:
                acertos += 1
                break

    return acertos >= 3


def mapear_colunas_do_cabecalho(colunas_entrada):
    mapeamento = {}
    usados = set()

    colunas_normalizadas = [normalizar_texto(c) for c in colunas_entrada]

    for coluna_padrao, sinonimos in MAPA_COLUNAS.items():
        sinonimos_norm = [normalizar_texto(s) for s in sinonimos]

        for idx, nome_coluna in enumerate(colunas_normalizadas):
            if idx in usados:
                continue
            if nome_coluna in sinonimos_norm:
                mapeamento[coluna_padrao] = idx
                usados.add(idx)
                break

    return mapeamento


def percentual_match_email(serie):
    s = serie.astype(str).str.strip()
    validos = s[s != ""]
    if len(validos) == 0:
        return 0
    return validos.str.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", na=False).mean()


def percentual_match_cpf(serie):
    s = serie.astype(str).apply(lambda x: re.sub(r"\D", "", x))
    validos = s[s != ""]
    if len(validos) == 0:
        return 0
    return validos.str.match(r"^\d{11}$", na=False).mean()


def percentual_match_data(serie):
    validos = parse_datas_serie(serie)
    validos = validos[validos.notna()]
    total = serie.astype(str).str.strip()
    total = total[(total != "") & (~total.isin(["nan", "NaN", "None", "NaT", "nat"]))]
    if len(total) == 0:
        return 0
    return len(validos) / len(total)


def percentual_match_carga_horaria(serie):
    s = serie.astype(str).str.strip()
    validos = s[s != ""]
    if len(validos) == 0:
        return 0

    extraido = validos.str.extract(r"(\d+)", expand=False)
    nums = pd.to_numeric(extraido, errors="coerce")

    return nums.between(1, 1000).fillna(False).mean()


def percentual_match_nome(serie):
    s = serie.astype(str).str.strip()
    validos = s[s != ""]
    if len(validos) == 0:
        return 0

    cond = (
        validos.str.contains(r"[A-Za-zÀ-ÿ]", regex=True, na=False)
        & ~validos.str.contains("@", na=False)
        & ~validos.str.match(r"^\d+$", na=False)
        & (validos.str.split().str.len() >= 2)
    )
    return cond.mean()


def pontuar_coluna(serie):
    return {
        "CPF": percentual_match_cpf(serie),
        "E-mail": percentual_match_email(serie),
        "Carga horária do certificado": percentual_match_carga_horaria(serie),
        "Data": percentual_match_data(serie),
        "Nome completo": percentual_match_nome(serie),
    }


def identificar_papeis_datas(colunas_indices, df):
    """
    Recebe os índices das 3 colunas de data e decide qual é:
    - Data de nascimento: a mais antiga
    - Início da participação: a menor entre as duas datas mais recentes
    - Término da participação: a maior entre as duas datas mais recentes
    """
    estatisticas = []

    for idx in colunas_indices:
        dt = parse_datas_serie(df.iloc[:, idx])

        validos = dt.dropna()
        if len(validos) == 0:
            raise ValueError("Não foi possível interpretar corretamente uma das colunas de data.")

        estatisticas.append({
            "idx": idx,
            "mediana": validos.median(),
            "min": validos.min(),
            "max": validos.max()
        })

    # Data de nascimento tende a ser a mais antiga de todas
    estatisticas_ordenadas = sorted(estatisticas, key=lambda x: x["mediana"])
    idx_nascimento = estatisticas_ordenadas[0]["idx"]

    restantes = estatisticas_ordenadas[1:]

    # Entre as duas restantes:
    # início deve ser a mais antiga / menor
    # término deve ser a mais recente / maior
    if restantes[0]["mediana"] <= restantes[1]["mediana"]:
        idx_inicio = restantes[0]["idx"]
        idx_termino = restantes[1]["idx"]
    else:
        idx_inicio = restantes[1]["idx"]
        idx_termino = restantes[0]["idx"]

    return {
        "Data de nascimento": idx_nascimento,
        "Início da participação": idx_inicio,
        "Término da participação": idx_termino
    }


def identificar_colunas_por_conteudo(df):
    candidatos = {}

    for idx in range(df.shape[1]):
        serie = df.iloc[:, idx]
        candidatos[idx] = pontuar_coluna(serie)

    usados = set()
    mapeamento = {}

    for campo in ["CPF", "E-mail", "Carga horária do certificado", "Nome completo"]:
        melhor_idx = None
        melhor_score = -1

        for idx, scores in candidatos.items():
            if idx in usados:
                continue
            score = scores[campo]
            if score > melhor_score:
                melhor_score = score
                melhor_idx = idx

        if melhor_idx is not None and melhor_score > 0:
            mapeamento[campo] = melhor_idx
            usados.add(melhor_idx)

    colunas_restantes = [idx for idx in range(df.shape[1]) if idx not in usados]

    colunas_data = []
    for idx in colunas_restantes:
        if candidatos[idx]["Data"] > 0:
            colunas_data.append(idx)

    if len(colunas_data) != 3:
        raise ValueError(
            f"Não foi possível identificar corretamente as 3 colunas de data. Encontradas: {len(colunas_data)}."
        )

    papeis_datas = identificar_papeis_datas(colunas_data, df)
    mapeamento.update(papeis_datas)

    return mapeamento


def reorganizar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.shape[1] != 7:
        raise ValueError(
            f"O arquivo precisa ter exatamente 7 colunas. Encontradas: {df.shape[1]}."
        )

    primeira_linha = df.iloc[0]

    if eh_cabecalho(primeira_linha):
        cabecalho = primeira_linha.tolist()
        df_dados = df.iloc[1:].copy().reset_index(drop=True)

        mapeamento = mapear_colunas_do_cabecalho(cabecalho)

        if len(mapeamento) < 7:
            mapeamento_conteudo = identificar_colunas_por_conteudo(df_dados)
            for coluna in COLUNAS_PADRAO:
                if coluna not in mapeamento:
                    mapeamento[coluna] = mapeamento_conteudo[coluna]

        # Ajuste extra: se o cabeçalho não nomear claramente as datas,
        # reavalia apenas as datas por conteúdo
        campos_data_presentes = [
            "Data de nascimento" in mapeamento,
            "Início da participação" in mapeamento,
            "Término da participação" in mapeamento
        ]

        if sum(campos_data_presentes) < 3:
            usados = {
                mapeamento[c]
                for c in mapeamento
                if c in ["CPF", "E-mail", "Carga horária do certificado", "Nome completo"]
            }
            colunas_restantes = [idx for idx in range(df_dados.shape[1]) if idx not in usados]
            papeis_datas = identificar_papeis_datas(colunas_restantes, df_dados)
            mapeamento.update(papeis_datas)

        df_reorganizado = pd.DataFrame()

        for coluna_padrao in COLUNAS_PADRAO:
            df_reorganizado[coluna_padrao] = df_dados.iloc[:, mapeamento[coluna_padrao]]

        return df_reorganizado

    else:
        df_dados = df.copy().reset_index(drop=True)
        mapeamento = identificar_colunas_por_conteudo(df_dados)

        df_reorganizado = pd.DataFrame()

        for coluna_padrao in COLUNAS_PADRAO:
            df_reorganizado[coluna_padrao] = df_dados.iloc[:, mapeamento[coluna_padrao]]

        return df_reorganizado


def tratar_dados(df: pd.DataFrame) -> pd.DataFrame:
    df = reorganizar_dataframe(df)

    df["Nome completo"] = df["Nome completo"].astype(str).str.strip()

    df["CPF"] = (
        df["CPF"]
        .astype(str)
        .apply(lambda x: re.sub(r"\D", "", x))
        .str.zfill(11)
    )

    df["E-mail"] = df["E-mail"].astype(str).str.strip().str.lower()

    for coluna in COLUNAS_DATA:
        df[coluna] = _converter_datas_para_ddmmaa(df[coluna])

    df["Carga horária do certificado"] = (
        df["Carga horária do certificado"]
        .astype(str)
        .str.extract(r"(\d+)", expand=False)
        .fillna("0")
    )

    return df.astype(str)


def ler_arquivo(uploaded_file):
    nome = uploaded_file.filename.lower()

    if nome.endswith(".xlsx"):
        return pd.read_excel(uploaded_file, header=None, dtype=str)

    raise ValueError("Formato inválido. Envie apenas arquivo XLSX.")


@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")


@app.route("/processar", methods=["POST"])
def processar():
    try:
        if "arquivo" not in request.files:
            flash("Nenhum arquivo foi enviado.")
            return redirect(url_for("index"))

        arquivo = request.files["arquivo"]

        if arquivo.filename == "":
            flash("Selecione um arquivo.")
            return redirect(url_for("index"))

        df = ler_arquivo(arquivo)

        linhas_remover = request.form.get("linhas_remover", "").strip()
        if linhas_remover:
            linhas_digitadas = [
                int(i.strip())
                for i in linhas_remover.split(",")
                if i.strip().isdigit()
            ]

            tem_cabecalho = False
            if not df.empty:
                tem_cabecalho = eh_cabecalho(df.iloc[0])

            if tem_cabecalho:
                indices_reais = [
                    i - 1
                    for i in linhas_digitadas
                    if i - 1 >= 0
                ]
            else:
                indices_reais = [
                    i - 2
                    for i in linhas_digitadas
                    if i - 2 >= 0
                ]

            if indices_reais:
                df = df.drop(index=indices_reais).reset_index(drop=True)

        df = tratar_dados(df)

        output = io.StringIO()
        df.to_csv(
            output,
            sep="\t",
            index=False,
            quoting=csv.QUOTE_ALL
        )

        mem = io.BytesIO()
        mem.write(output.getvalue().encode("utf-8"))
        mem.seek(0)

        nome_saida = arquivo.filename.rsplit(".", 1)[0] + "_formatado.csv"

        return send_file(
            mem,
            as_attachment=True,
            download_name=nome_saida,
            mimetype="text/csv"
        )

    except Exception as e:
        flash(f"Ocorreu um erro: {str(e)}")
        return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(debug=True)
