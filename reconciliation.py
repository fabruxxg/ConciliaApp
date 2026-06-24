"""
Motor de conciliación server-side — 4 canales: Infonet, Netel, Pronet, Compras.
Portado exactamente desde la lógica JS de ConciliaAppXX.html.
"""
import re
import io
import datetime
import pandas as pd
from lxml import etree
from typing import Optional


# ══ HELPERS ══════════════════════════════════════════════════════════════════

def num(v) -> float:
    """Parsea número en formato paraguayo: punto=miles, coma=decimal."""
    if v is None or str(v).strip() == '':
        return 0.0
    s = str(v).strip()
    if re.match(r'^-?\d{1,3}(\.\d{3})+(,\d+)?$', s) or re.match(r'^-?\d+(,\d+)$', s):
        s = s.replace('.', '').replace(',', '.')
    else:
        s = re.sub(r'[^0-9.\-]', '', s)
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def netel_id(raw: str) -> str:
    s = str(raw).strip()
    if len(s) <= 7:
        return s
    n = s[1:] if s.startswith('1') else s
    if len(n) >= 9:
        return f"001-{n[:3].zfill(3)}-{n[3:].zfill(7)}"
    return s


def norm_nro(n) -> str:
    s = re.sub(r'[^0-9]', '', str(n or ''))
    return s.lstrip('0') or '0'


def norm_ruc(r: str) -> str:
    s = str(r or '').strip()
    s = re.sub(r'-\d{1,2}$', '', s)
    digits = re.sub(r'[^0-9]', '', s)
    return digits or s.strip()


def excel_date_to_str(v) -> str:
    if v is None or str(v).strip() == '':
        return ''
    s = str(v).strip()
    if re.match(r'^\d{4}-\d{2}-\d{2}', s):
        return s.split(' ')[0]
    m = re.match(r'^(\d{2})/(\d{2})/(\d{4})', s)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    try:
        n = float(s)
        if n > 1000:
            d = datetime.datetime(1899, 12, 30) + datetime.timedelta(days=int(n))
            return d.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        pass
    return s.split(' ')[0]


def _read_sheet(file_bytes: bytes, skip_rows: int = 0) -> list:
    """Lee primer sheet de Excel, retorna lista de dicts con strings."""
    df = pd.read_excel(io.BytesIO(file_bytes), skiprows=skip_rows, dtype=str, header=0)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.fillna('')
    return df.to_dict('records')


# ══ INFONET ══════════════════════════════════════════════════════════════════

def concil_infornet(portal_bytes: bytes, sys_bytes: bytes) -> list:
    # Portal: CSV ISO-8859-1, separador punto y coma
    text = portal_bytes.decode('iso-8859-1')
    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        return []
    hdrs = [h.strip() for h in lines[0].split(';')]
    portal = []
    for line in lines[1:]:
        vals = line.split(';')
        obj = {hdrs[i]: (vals[i].strip() if i < len(vals) else '') for i in range(len(hdrs))}
        if obj.get('Nro. transaccion'):
            portal.append(obj)

    # Sistema: XLSX filtrado por Cód=30 o Comprobante contiene INFONET
    sys_rows = _read_sheet(sys_bytes)
    sys_filtered = [
        r for r in sys_rows
        if str(r.get('Cód. Comprobante', '')).strip().rstrip('.0') == '30'
        or 'INFONET' in str(r.get('Comprobante', '')).upper()
    ]

    s_map: dict = {}
    for r in sys_filtered:
        t = str(r.get('Transacción', '')).strip().rstrip('.0')
        if t:
            s_map.setdefault(t, []).append(r)

    results = []
    for r in portal:
        trx = str(r.get('Nro. transaccion', '')).strip().rstrip('.0')
        imp = num(r.get('Importe', 0))
        s_arr = s_map.get(trx, [])
        i_s = sum(num(x.get('Importe Total', 0)) for x in s_arr) if s_arr else None
        diff = (imp - i_s) if i_s is not None else 0
        detalle = [{'nro': str(x.get('Numero Factura', '')).strip(), 'importe': num(x.get('Importe Total', 0))} for x in s_arr]
        facturas = [d['nro'] for d in detalle if d['nro']]
        facturas_dif = facturas if (i_s is not None and abs(diff) > 0.5) else []
        results.append({
            'sistema': 'infonet',
            'fecha': r.get('Fecha de venta', ''),
            'trx': trx, 'nroRaw': trx,
            'cliente': str(s_arr[0].get('Cliente', '')).strip() if s_arr else str(r.get('Empresa', '')).strip(),
            'ruc': str(s_arr[0].get('RUC', '')).strip() if s_arr else '',
            'factura': ' / '.join(facturas),
            'facturas': facturas, 'facturasDif': facturas_dif, 'facturaDetalle': detalle,
            'impPortal': imp, 'impSys': i_s, 'diff': diff,
            'sinCruz': i_s is None,
            'estado': r.get('Estado', ''),
            'causa': 'Desvío de importe detectado' if (i_s is not None and abs(diff) > 0.5) else '',
        })
    return results


# ══ NETEL ════════════════════════════════════════════════════════════════════

def concil_netel(portal_bytes: bytes, sys_bytes: bytes) -> list:
    # Portal: XLSX, saltear primera fila (skip=1 en JS)
    raw = _read_sheet(portal_bytes, skip_rows=1)
    if not raw:
        return []

    col_id = next((k for k in raw[0] if 'dato' in k.lower()), 'Dato id')
    col_t  = next((k for k in raw[0] if k.lower() == 'total'), 'Total')
    col_f  = next((k for k in raw[0] if k.lower().startswith('fec')), 'Fecha')

    agg_p: dict = {}
    for r in raw:
        raw_id = str(r.get(col_id, '')).strip()
        if not raw_id or raw_id == col_id:
            continue
        id_ = netel_id(raw_id)
        if id_ not in agg_p:
            agg_p[id_] = {'total': 0.0, 'fecha': str(r.get(col_f, '')).strip()}
        agg_p[id_]['total'] += num(r.get(col_t, 0))

    sys_rows = _read_sheet(sys_bytes)
    sys_filtered = [
        r for r in sys_rows
        if str(r.get('Cód. Comprobante', '')).strip().rstrip('.0') == '40'
        or 'EXPRESS' in str(r.get('Comprobante', '')).upper()
    ]

    a_r: dict = {}
    a_f: dict = {}
    for r in sys_filtered:
        ruc = str(r.get('RUC', '')).strip()
        fac = str(r.get('Numero Factura', '')).strip()
        tot = num(r.get('Importe Total', 0))
        cli = str(r.get('Cliente', '')).strip()
        if ruc:
            if ruc not in a_r:
                a_r[ruc] = {'total': 0.0, 'facturas': [], 'cliente': cli, 'ruc': ruc}
            a_r[ruc]['total'] += tot
            if fac:
                a_r[ruc]['facturas'].append({'nro': fac, 'importe': tot})
        if fac:
            if fac not in a_f:
                a_f[fac] = {'total': 0.0, 'facturas': [], 'cliente': cli, 'ruc': ruc}
            a_f[fac]['total'] += tot
            a_f[fac]['facturas'].append({'nro': fac, 'importe': tot})

    results = []
    for id_, pd_ in agg_p.items():
        s = a_r.get(id_) or a_f.get(id_)
        i_s = s['total'] if s else None
        diff = (pd_['total'] - i_s) if i_s is not None else 0
        detalle = s['facturas'] if s else []
        facturas = [d['nro'] for d in detalle if d['nro']]
        facturas_dif = facturas if (i_s is not None and abs(diff) > 0.5) else []
        results.append({
            'sistema': 'netel',
            'fecha': pd_['fecha'], 'trx': id_, 'nroRaw': id_,
            'cliente': s['cliente'] if s else '',
            'ruc': (s['ruc'] if s else id_) or id_,
            'factura': ' / '.join(facturas),
            'facturas': facturas, 'facturasDif': facturas_dif, 'facturaDetalle': detalle,
            'impPortal': pd_['total'], 'impSys': i_s, 'diff': diff,
            'sinCruz': i_s is None, 'estado': 'CO',
            'causa': 'Desvío de importe detectado' if (i_s is not None and abs(diff) > 0.5) else '',
        })
    return results


# ══ PRONET ═══════════════════════════════════════════════════════════════════

def concil_pronet(portal_bytes: bytes, sys_bytes: bytes) -> list:
    # Portal: HTML con tabla
    portal = []
    try:
        html_text = portal_bytes.decode('utf-8')
        doc = etree.fromstring(html_text.encode('utf-8'), parser=etree.HTMLParser())
        tables = doc.xpath('//table')
        if tables:
            table = tables[0]
            hdrs = [th.text_content().strip() if hasattr(th, 'text_content') else (th.text or '') for th in table.xpath('.//thead//th')]
            for tr in table.xpath('.//tbody//tr'):
                tds = tr.xpath('.//td')
                if not tds:
                    continue
                obj = {}
                for i, h in enumerate(hdrs):
                    obj[h] = tds[i].text_content().strip() if i < len(tds) else ''
                if obj.get('Banco'):
                    portal.append(obj)
    except Exception as e:
        print(f'Error portal Pronet: {e}')

    sys_rows = _read_sheet(sys_bytes)
    sys_filtered = []
    for r in sys_rows:
        if str(r.get('Fecha', '')).strip().upper() == 'TOTAL':
            continue
        raw_cod = str(r.get('Cód. Comprobante', r.get('Cod. Comprobante', ''))).strip()
        try:
            cod = int(float(raw_cod))
        except (ValueError, TypeError):
            cod = 0
        comp = str(r.get('Comprobante', '')).upper()
        if cod == 20 or 'PRONET' in comp:
            sys_filtered.append(r)

    s_map: dict = {}
    for r in sys_filtered:
        raw_t = str(r.get('Transacción', '')).strip()
        try:
            t = str(int(float(raw_t))) if raw_t else ''
        except (ValueError, TypeError):
            t = raw_t
        if t and t != 'nan':
            s_map.setdefault(t, []).append(r)

    results = []
    for r in portal:
        raw_trx = str(r.get('Codigo Trx.', '')).strip()
        try:
            trx = str(int(float(raw_trx))) if raw_trx else ''
        except (ValueError, TypeError):
            trx = raw_trx
        if not trx or trx == 'nan':
            continue
        imp = num(r.get('Importe', 0))
        est = str(r.get('Confirmado/Anulado', '')).strip()
        s_arr = s_map.get(trx, [])
        i_s = sum(num(x.get('Importe Total', 0)) for x in s_arr) if s_arr else None
        diff = (imp - i_s) if i_s is not None else 0
        detalle = [{'nro': str(x.get('Numero Factura', '')).strip(), 'importe': num(x.get('Importe Total', 0))} for x in s_arr]
        facturas = [d['nro'] for d in detalle if d['nro']]
        facturas_dif = facturas if (i_s is not None and abs(diff) > 0.5) else []
        fallback_fac = str(r.get('Referencia Pago', '')).strip().lstrip("'")
        results.append({
            'sistema': 'pronet',
            'fecha': str(r.get('Fecha Cobro', '')).strip(),
            'trx': trx, 'nroRaw': trx,
            'cliente': str(s_arr[0].get('Cliente', '')).strip() if s_arr else '',
            'ruc': str(s_arr[0].get('RUC', '')).strip() if s_arr else '',
            'factura': ' / '.join(facturas) if facturas else fallback_fac,
            'facturas': facturas if facturas else ([fallback_fac] if fallback_fac else []),
            'facturasDif': facturas_dif, 'facturaDetalle': detalle,
            'impPortal': imp, 'impSys': i_s, 'diff': diff,
            'sinCruz': i_s is None, 'estado': est,
            'causa': 'Desvío de importe detectado' if (i_s is not None and abs(diff) > 0.5) else '',
        })
    return results


# ══ COMPRAS (Marangatu vs Libro) ═════════════════════════════════════════════

def concil_compras(marangatu_bytes: bytes, libro_bytes: bytes, fecha_corte: Optional[str] = None) -> list:

    # ── 1. Parsear Marangatu (CSV o XLSX, detección dinámica de columnas) ──
    sample = marangatu_bytes[:1024]
    try:
        first_line = sample.decode('utf-8').split('\n')[0]
    except Exception:
        first_line = sample.decode('iso-8859-1', errors='replace').split('\n')[0]
    sep = ';' if ';' in first_line else ','

    try:
        wb_m = pd.read_excel(io.BytesIO(marangatu_bytes), header=None, dtype=str)
        data_m = wb_m.fillna('').values.tolist()
    except Exception:
        try:
            text = marangatu_bytes.decode('utf-8')
        except Exception:
            text = marangatu_bytes.decode('iso-8859-1', errors='replace')
        import csv as csv_mod
        reader = csv_mod.reader(text.splitlines(), delimiter=sep)
        data_m = [row for row in reader]

    # Detectar fila de encabezado: la que tiene más palabras clave
    keywords = {'ruc', 'comprobante', 'fecha', 'timbrado', 'grav', 'iva', 'nombre',
                'razon', 'tipo', 'exenta', 'total', 'nro', 'número', 'numero', 'identificaci'}
    h_m = 0
    best_hits = 0
    for i, row in enumerate(data_m[:10]):
        lower = [str(c).lower().strip() for c in row]
        hits = sum(1 for h in lower if h and any(kw in h for kw in keywords))
        if hits > best_hits:
            best_hits = hits
            h_m = i

    # Mapear columnas
    c_ruc = c_nom = c_fecha = c_tipo = c_nro = c_timb = -1
    c_grav5 = c_grav10 = c_exenta = c_iva5 = c_iva10 = c_total = -1
    if best_hits >= 3:
        hrow = [str(c).lower().strip() for c in data_m[h_m]]
        for j, h in enumerate(hrow):
            if ('ruc' in h or ('identificaci' in h and 'comprobante' not in h)) and c_ruc < 0:
                c_ruc = j
            elif ('nombre' in h or 'razon' in h or 'razón' in h) and c_nom < 0:
                c_nom = j
            elif 'fecha' in h and c_fecha < 0:
                c_fecha = j
            elif 'tipo' in h and c_tipo < 0:
                c_tipo = j
            elif 'timbrado' in h and c_timb < 0:
                c_timb = j
            elif ('identificador' not in h and 'identificaci' not in h and
                  ('comprobante' in h or 'número' in h or 'numero' in h or 'nro' in h)) and c_nro < 0:
                c_nro = j
            elif 'grav' in h and '5' in h and c_grav5 < 0:
                c_grav5 = j
            elif 'grav' in h and '10' in h and c_grav10 < 0:
                c_grav10 = j
            elif ('exenta' in h or 'exento' in h) and c_exenta < 0:
                c_exenta = j
            elif 'iva' in h and '5' in h and c_iva5 < 0:
                c_iva5 = j
            elif 'iva' in h and '10' in h and c_iva10 < 0:
                c_iva10 = j
            elif ('monto' in h and 'total' in h or h == 'total') and c_total < 0:
                c_total = j

    # Fallbacks layout típico Marangatu COMPRAS
    def fb(v, d): return v if v >= 0 else d
    c_ruc = fb(c_ruc, 1); c_nom = fb(c_nom, 2); c_fecha = fb(c_fecha, 3)
    c_tipo = fb(c_tipo, 4); c_nro = fb(c_nro, 5); c_timb = fb(c_timb, 6)
    c_grav5 = fb(c_grav5, 7); c_grav10 = fb(c_grav10, 8); c_exenta = fb(c_exenta, 9)
    c_iva5 = fb(c_iva5, 10); c_iva10 = fb(c_iva10, 11); c_total = fb(c_total, 7)

    def cell(row, idx):
        try:
            return str(row[idx]).strip() if idx < len(row) else ''
        except Exception:
            return ''

    map_m = {}
    cnt_no_fact = cnt_corte = cnt_dup = 0
    for row in data_m[h_m + 1:]:
        if all(not str(c).strip() for c in row):
            continue
        tipo = cell(row, c_tipo).lower()
        if tipo and not tipo.startswith('factura'):
            cnt_no_fact += 1
            continue
        fecha_m = excel_date_to_str(cell(row, c_fecha))
        if fecha_corte and fecha_m and fecha_m > fecha_corte:
            cnt_corte += 1
            continue
        ruc = norm_ruc(cell(row, c_ruc))
        nro = cell(row, c_nro)
        if not ruc or not nro:
            continue
        key = norm_nro(nro) + '_' + ruc
        if key in map_m:
            cnt_dup += 1
            continue
        g5 = num(cell(row, c_grav5)); g10 = num(cell(row, c_grav10))
        ex = num(cell(row, c_exenta))
        i5 = num(cell(row, c_iva5)); i10 = num(cell(row, c_iva10))
        tot = num(cell(row, c_total))
        monto = tot if tot > 0 else g5 + i5 + g10 + i10 + ex
        map_m[key] = {
            'ruc': ruc, 'nombre': cell(row, c_nom),
            'fecha': fecha_m, 'tipo': cell(row, c_tipo) or 'FACTURA',
            'nro': norm_nro(nro), 'nroRaw': nro,
            'timbrado': cell(row, c_timb), 'monto': monto,
        }

    # ── 2. Parsear Libro de Compras (XLSX) ──
    wb_l_raw = pd.ExcelFile(io.BytesIO(libro_bytes))
    sh_name = wb_l_raw.sheet_names[0]
    for n in wb_l_raw.sheet_names:
        if re.search(r'econt|compra', n, re.I):
            sh_name = n
            break
    data_l_df = pd.read_excel(io.BytesIO(libro_bytes), sheet_name=sh_name, header=None, dtype=str)
    data_l = data_l_df.fillna('').values.tolist()

    h_l = -1
    c_nomcom = c_nro_l = c_fecha_l = c_grav5_l = c_grav10_l = c_exenta_l = -1
    c_iva5_l = c_iva10_l = c_nomcli = c_prov = c_timb_l = -1
    for i, row in enumerate(data_l[:5]):
        lower = [str(c).lower().strip() for c in row]
        if any(h in ('nomcom', 'nrocomp', 'proveedor') for h in lower):
            h_l = i
            for j, h in enumerate(lower):
                if h in ('nomcom', 'tipo') and c_nomcom < 0:
                    c_nomcom = j
                elif h in ('nrocomp', 'numero', 'número') or (re.search(r'nro.*comp', h)) and c_nro_l < 0:
                    c_nro_l = j
                elif 'fecha' in h and c_fecha_l < 0:
                    c_fecha_l = j
                elif h == 'gravada5' or h == 'grav5':
                    c_grav5_l = j
                elif h == 'gravada10' or h == 'grav10':
                    c_grav10_l = j
                elif h in ('exenta', 'exento'):
                    c_exenta_l = j
                elif h == 'iva5':
                    c_iva5_l = j
                elif h == 'iva10':
                    c_iva10_l = j
                elif h in ('nomcli',) or 'razón' in h or 'razon' in h:
                    c_nomcli = j
                elif h in ('ruc', 'proveedor') and c_prov < 0:
                    c_prov = j
                elif 'timbrado' in h:
                    c_timb_l = j
            break

    # Fallback layout FoxPro librocompra
    if h_l < 0:
        h_l = 0
        c_nomcom = 0; c_nro_l = 1; c_fecha_l = 2; c_grav5_l = 3; c_grav10_l = 4
        c_exenta_l = 5; c_nomcli = 6; c_prov = 7; c_timb_l = 12

    def fb(v, d): return v if v >= 0 else d
    c_nro_l = fb(c_nro_l, 1); c_prov = fb(c_prov, 3); c_grav5_l = fb(c_grav5_l, 4)
    c_grav10_l = fb(c_grav10_l, 5); c_exenta_l = fb(c_exenta_l, 6)

    map_l = {}
    for row in data_l[h_l + 1:]:
        fecha_lv = excel_date_to_str(cell(row, c_fecha_l))
        if fecha_corte and fecha_lv and fecha_lv > fecha_corte:
            continue
        nro = cell(row, c_nro_l)
        prov = norm_ruc(cell(row, c_prov))
        if not nro or not prov or nro == '0':
            continue
        grav5 = num(cell(row, c_grav5_l)); grav10 = num(cell(row, c_grav10_l))
        exenta = num(cell(row, c_exenta_l))
        iva5 = num(cell(row, c_iva5_l)) if c_iva5_l >= 0 else round(grav5 * 0.05)
        iva10 = num(cell(row, c_iva10_l)) if c_iva10_l >= 0 else round(grav10 * 0.10)
        total = grav5 + iva5 + grav10 + iva10 + exenta
        key = norm_nro(nro) + '_' + prov
        if key in map_l:
            continue
        map_l[key] = {
            'tipo': cell(row, c_nomcom) or 'FACTURA',
            'nro': norm_nro(nro), 'nroRaw': nro, 'prov': prov,
            'nomcli': cell(row, c_nomcli) if c_nomcli >= 0 else '',
            'total': total, 'grav5': grav5, 'grav10': grav10, 'exenta': exenta,
            'timb': cell(row, c_timb_l) if c_timb_l >= 0 else '',
            'fecha': fecha_lv,
        }

    # ── 3. Cruce ──
    rows = []
    matched_l = set()

    for key, m in map_m.items():
        l = map_l.get(key)
        i_s = l['total'] if l else None
        diff = (m['monto'] - i_s) if i_s is not None else m['monto']
        sin_cruz = i_s is None
        causa = ''
        if sin_cruz:
            causa = 'Comprobante en Marangatu sin registro en Libro'
        elif abs(diff) > 0.5:
            causa = f"Diferencia de importe (Gs. {abs(diff):,.0f})"
        if l:
            matched_l.add(key)
        rows.append({
            'sistema': 'compras', 'fecha': m['fecha'], 'trx': m['nro'], 'nroRaw': m['nroRaw'],
            'cliente': m['nombre'], 'ruc': m['ruc'], 'factura': m['timbrado'],
            'facturas': [], 'facturasDif': [], 'facturaDetalle': [],
            'impPortal': m['monto'], 'impSys': i_s, 'diff': diff,
            'sinCruz': sin_cruz, 'estado': m['tipo'], 'causa': causa,
        })

    for key, l in map_l.items():
        if key in matched_l:
            continue
        rows.append({
            'sistema': 'compras', 'fecha': l['fecha'], 'trx': l['nro'], 'nroRaw': l['nroRaw'],
            'cliente': l['nomcli'], 'ruc': l['prov'], 'factura': l['timb'],
            'facturas': [], 'facturasDif': [], 'facturaDetalle': [],
            'impPortal': None, 'impSys': l['total'], 'diff': -l['total'],
            'sinCruz': False, 'estado': l['tipo'],
            'causa': 'Comprobante en Libro sin registro en Marangatu',
        })

    return rows
