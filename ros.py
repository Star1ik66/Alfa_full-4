import csv
import os
import re
from tqdm import tqdm

# ---------- твои настройки (оставь как есть) ----------
INPUT_CSV_FILE = 'contacts_merged.csv'
WHATSAPP_RESULTS_CSV = 'results.csv'
# если хочешь принудительно задать имена, можно, но ниже есть автоопределение
WA_CSV_PHONE_COLUMN = None      # 'Телефон'  # ставь None, чтобы включить автоопределение
WA_CSV_STATUS_COLUMN = None     # 'WhatsApp'
WA_CSV_POSITIVE_STATUS = 'ДА'
OUTPUT_CSV_FILE = 'numb1.csv'
PHONE_COLUMN_NAME = 'phone2'
NEW_COLUMN_NAME = 'WA'
# -------------------------------------------------------

def format_phone(phone_string: str) -> str | None:
    if phone_string is None:
        return None
    s = str(phone_string).strip()
    if s == '':
        return None
    # Оставляем только цифры и плюс
    cleaned = re.sub(r'[^\d\+]', '', s)
    # Если есть ведущий +7 или +, убираем, затем нормируем
    cleaned = cleaned.lstrip('+')
    # Иногда номер приходит в виде 8XXXXXXXXXX или 7XXXXXXXXXX или 9XXXXXXXXX
    if len(cleaned) == 11 and (cleaned.startswith('7') or cleaned.startswith('8')):
        cleaned = cleaned[1:]
    if len(cleaned) == 10:
        return '+7' + cleaned
    # Если длина 9 (в редких случаях) — не нормализуем
    return None

def detect_columns_and_iter_rows(filename: str, max_preview_rows: int = 10):
    """
    Пробегает первые несколько строк CSV, пытается найти индекс столбца с телефоном и со статусом.
    Возвращает (phone_idx, status_idx, header_row_index, rows_generator)
    """
    phone_candidates = ['телефон', 'номер', 'phone', 'tel', 'phone_number']
    status_candidates = ['whatsapp', 'ошибка', 'status', 'статус', 'result']

    # Откроем файл и прочитаем первые N строк
    with open(filename, 'r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f)
        preview = []
        for i, row in enumerate(reader):
            preview.append(row)
            if i >= max_preview_rows - 1:
                break

    # Попробуем найти строку, которая выглядит как заголовок (с кандидатами)
    header_row_index = None
    phone_idx = None
    status_idx = None

    for idx, row in enumerate(preview):
        lowered = [c.strip().lower() for c in row]
        # поиск индекса телефона
        for i_col, col in enumerate(lowered):
            if any(pc in col for pc in phone_candidates):
                phone_idx = i_col
                break
        for i_col, col in enumerate(lowered):
            if any(sc in col for sc in status_candidates):
                status_idx = i_col
                break
        # если нашли оба — считаем эту строку заголовком
        if phone_idx is not None and status_idx is not None:
            header_row_index = idx
            break

    # Если не нашли оба индекса — попробуем более свободный поиск:
    if header_row_index is None:
        # ищем любую строку из preview, где хотя бы есть кандидат phone
        for idx, row in enumerate(preview):
            lowered = [c.strip().lower() for c in row]
            if any(any(pc in col for pc in phone_candidates) for col in lowered):
                header_row_index = idx
                # назначим phone_idx приблизительно
                for i_col, col in enumerate(lowered):
                    if any(pc in col for pc in phone_candidates):
                        phone_idx = i_col
                        break
                for i_col, col in enumerate(lowered):
                    if any(sc in col for sc in status_candidates):
                        status_idx = i_col
                        break
                break

    # Создадим генератор строк, пропуская все строки до header_row_index (включая сам header)
    def rows_gen():
        with open(filename, 'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if header_row_index is not None and i <= header_row_index:
                    # пропускаем заголовок и всё до него
                    continue
                yield row

    return phone_idx, status_idx, header_row_index, preview, rows_gen()

def load_whatsapp_phones_from_csv(filename: str, phone_col_name: str | None, status_col_name: str | None, positive_status: str) -> set | None:
    if not os.path.exists(filename):
        print(f"Ошибка: файл '{filename}' не найден.")
        return None

    print(f"Попытка загрузить телефоны с WhatsApp из '{filename}'...")
    # Первый этап: автоматическое определение колонок
    phone_idx, status_idx, header_row_index, preview_rows, rows_iter = detect_columns_and_iter_rows(filename)

    # Если параметры заданы вручную, попробуем использовать их -> иначе используем индекс
    use_dict_reader = False
    if phone_col_name and status_col_name:
        # попробуем открыть как DictReader — если такие колонки есть, используем их
        with open(filename, 'r', encoding='utf-8', newline='') as f:
            dr = csv.DictReader(f)
            hdr = dr.fieldnames or []
            lowered = [h.strip().lower() for h in hdr]
            if phone_col_name.strip().lower() in lowered and status_col_name.strip().lower() in lowered:
                use_dict_reader = True

    phones_with_wa = set()
    # Если DictReader подходит — используем его (удобнее)
    if use_dict_reader:
        with open(filename, 'r', encoding='utf-8', newline='') as f:
            dr = csv.DictReader(f)
            # Приведём имена колонок к нормализованным (для поиска)
            header_map = {h.strip(): h for h in dr.fieldnames}  # original names
            for row in dr:
                status = row.get(status_col_name, "")
                phone_raw = row.get(phone_col_name, "")
                if status is None:
                    continue
                if str(status).strip().lower() == positive_status.lower():
                    formatted = format_phone(phone_raw)
                    if formatted:
                        phones_with_wa.add(formatted)
    else:
        # Работаем по индексам (если были найдены)
        if phone_idx is None:
            print("Не удалось автоматически определить колонку с телефоном. Пожалуйста, откройте файл и проверьте названия колонок.")
            print("Первые строки файла (preview):")
            for r in preview_rows:
                print(r)
            return None
        # Если статус не определён по индексу — попробуем догадаться: он может быть последней колонкой
        if status_idx is None:
            # попробуем взять последнюю колонку
            status_idx = -1

        # Итерируем все строки по rows_iter
        for row in rows_iter:
            # защищаем от коротких строк
            if len(row) <= abs(phone_idx) - 1:
                continue
            try:
                phone_raw = row[phone_idx]
            except Exception:
                phone_raw = None
            try:
                status_raw = row[status_idx]
            except Exception:
                status_raw = ""
            if status_raw is None:
                continue
            if str(status_raw).strip().lower() == positive_status.lower():
                formatted = format_phone(phone_raw)
                if formatted:
                    phones_with_wa.add(formatted)

    print(f"Загружено {len(phones_with_wa)} уникальных номеров с WhatsApp (формат +7...).")
    # Показать примеры
    sample = list(phones_with_wa)[:10]
    print("Примеры номеров из results.csv (после форматирования):")
    for s in sample:
        print(" ", s)
    return phones_with_wa

def main():
    phones_with_wa = load_whatsapp_phones_from_csv(
        WHATSAPP_RESULTS_CSV,
        WA_CSV_PHONE_COLUMN,
        WA_CSV_STATUS_COLUMN,
        WA_CSV_POSITIVE_STATUS
    )
    if phones_with_wa is None:
        return

    if not os.path.exists(INPUT_CSV_FILE):
        print(f"Ошибка: основной файл '{INPUT_CSV_FILE}' не найден.")
        return

    # Сначала соберём множество всех номеров из основного файла (PHONE_COLUMN_NAME), чтобы быстро диагностировать несовпадения
    main_phones_set = set()
    total_rows = 0
    with open(INPUT_CSV_FILE, 'r', encoding='utf-8', newline='') as inf:
        rdr = csv.DictReader(inf)
        if PHONE_COLUMN_NAME not in (rdr.fieldnames or []):
            print(f"В основном файле нет колонки '{PHONE_COLUMN_NAME}'. Доступные колонки: {rdr.fieldnames}")
            return
        for r in rdr:
            total_rows += 1
            p = r.get(PHONE_COLUMN_NAME)
            fp = format_phone(p)
            if fp:
                main_phones_set.add(fp)

    print(f"В основном файле '{INPUT_CSV_FILE}' найдено {len(main_phones_set)} уникальных номеров в колонке '{PHONE_COLUMN_NAME}' (всего строк: {total_rows}).")

    # Диагностика: примеры номеров из results.csv которые НЕ найдены в основном файле
    not_found = []
    found_count = 0
    for p in list(phones_with_wa)[:500]:  # проверим первые 500 для примера
        if p in main_phones_set:
            found_count += 1
        else:
            not_found.append(p)
        if len(not_found) >= 10:
            break

    print(f"Из примера до 500 номеров из results.csv: найдено в основном файле: {found_count}, не найдено примеров: {len(not_found)}")
    if not_found:
        print("Примеры номеров из results.csv не найденных в основном файле (первые 10):")
        for nf in not_found:
            print(" ", nf)

    # Если всё ок, продолжаем и создаём итоговый файл
    # Оригинальная логика записи
    with open(INPUT_CSV_FILE, mode='r', newline='', encoding='utf-8') as infile, \
         open(OUTPUT_CSV_FILE, mode='w', newline='', encoding='utf-8') as outfile:

        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames or []
        if NEW_COLUMN_NAME not in fieldnames:
            fieldnames = fieldnames + [NEW_COLUMN_NAME]

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        processed = 0
        have_wa = 0
        no_wa = 0
        for row in tqdm(reader, desc="Запись строк"):
            phone_from_csv = row.get(PHONE_COLUMN_NAME)
            formatted_phone = format_phone(phone_from_csv)

            if formatted_phone and formatted_phone in phones_with_wa:
                row[NEW_COLUMN_NAME] = 'есть ватсап'
                have_wa += 1
            else:
                row[NEW_COLUMN_NAME] = 'нету'
                no_wa += 1
            writer.writerow(row)
            processed += 1

    print("\nГотово!")
    print(f"Обработано строк: {processed}")
    print(f"Найдено с WhatsApp: {have_wa}")
    print(f"Без WhatsApp: {no_wa}")
    print(f"Результат сохранён в: {OUTPUT_CSV_FILE}")

if __name__ == "__main__":
    main()
