from datetime import time, timedelta, datetime
import pandas as pd
import logging
import shutil
import csv
import os
import re

ham_folder = r"home/temizlenmis"
tab_folder = r"home/okunabilir"
destination_folder = r"home/empty_gps"
log_file = r"home/empty_gps/empty_gps.log"


def move_empty_tab_files(log_file, source_folder, target_folder):
    
    with open(log_file, "r") as file:
        lines = file.readlines()

    for line in lines:
        if ".tab" in line:
            file_name = line.split("Veri Adi: ")[-1].strip()
            if os.path.exist(os.path.join(source_folder, file_name)):
                shutil.move(os.path.join(source_folder, file_name), os.path.join(target_folder, file_name))
                print(f"Dosya tasindi: {file_name} -> {target_folder}")
            else:
                print(f"Dosya bulunamadi: {file_name}")



def tarih_format(tarih):
    
    if pd.notna(tarih) and isinstance(tarih, str):
        return tarih.replace('.', '-')

    return tarih



def partition_index_add(data, tab_data_baslangic, batch_size = 1000):

    toplam_satir  = len(data)

    aralik_sutunu = []

    for i in range(toplam_satir):
        
        aralik_baslangic = ((tab_data_baslangic + i) // batch_size) * batch_size
        aralik_bitis     = aralik_baslangic + batch_size

        aralik_sutunu.append(f"{aralik_baslangic} - {aralik_bitis}")

    data.insert(3, "Partition_Index", aralik_sutunu)

    return data



def tablo_islem(tab_data):
    
    if "Port 1 Makine Tarihi" in tab_data.columns and "Port 2 Makine Tarihi" in tab_data.columns:

        tab_data["Port 1 Makine Tarihi"] = tab_data["Port 1 Makine Tarihi"].fillna(tab_data["Port 2 Makine Tarihi"])
        tab_data["Port 2 Makine Tarihi"] = tab_data["Port 2 Makine Tarihi"].fillna(tab_data["Port 1 Makine Tarihi"])

        tab_data["Port 1 Makine Tarihi"] = tab_data["Port 1 Makine Tarihi"].apply(tarih_format)
        tab_data["Port 2 Makine Tarihi"] = tab_data["Port 2 Makine Tarihi"].apply(tarih_format)

    if "Port 1 Makine Zamani" in tab_data.columns and "Port 2 Makine Zamani" in tab_data.columns:

        tab_data["Port 1 Makine Zamani"] = tab_data["Port 1 Makine Zamani"].fillna(tab_data["Port 2 Makine Zamani"])
        tab_data["Port 2 Makine Zamani"] = tab_data["Port 2 Makine Zamani"].fillna(tab_data["Port 1 Makine Zamani"])



def kayit_tipi_belirleme(file_name):
    
    pattern = r"YKI\d+_(.*)"
    match = re.search(pattern, file_name)  # Deseni dosya adında ara

    if match:
        
        return match.group(1).replace("_", " ").strip()
    
    return None                             # Eğer "YKI" ve ardından gelen sayı bulunamazsa



def ham_tab_eslestir(ham_folder, tab_file):

    ham_files = [f for f in os.listdir(ham_folder) if f.endswith(".ham")]

    for ham_file in ham_files:
        
        ham_anahtar = "_".join(ham_file.split("_")[:-5])

        if tab_file.startswith(ham_anahtar):
            
            pattern = r"BAH_ID_(.*)"
            match   = re.search(pattern, ham_file)

            if match:
                bah_id = match.group(1).replace(".ham", "").strip()
                return bah_id
            


def gps_zamani_hesapla(gps_week, gps_ms):
    """
    GPS haftası ve milisaniyesini UTC tarih ve saatine dönüştürür.

    Args:
        gps_week (int): GPS haftası.
        gps_ms (int)  : GPS milisaniyesi.

    Returns:
        tuple: UTC tarihi (datetime.date) ve UTC saati (datetime.time) olarak döner.
    """
    gps_epoch = datetime(1980, 1, 6)  # GPS zamanının başlangıç tarihi
    gps_to_utc_offset = 18  # GPS-UTC arasındaki fark (saniye cinsinden)

    # GPS saniyelerini hesapla
    gps_seconds = (gps_week * 7 * 86400) + (gps_ms / 1000)

    # GPS-UTC farkını çıkararak UTC saniyelerini hesapla
    utc_seconds = gps_seconds - gps_to_utc_offset

    # UTC tarih ve saatine çevir
    utc_time = gps_epoch + timedelta(seconds=utc_seconds)

    # Tarih ve saat olarak ayır
    gps_tarihi = utc_time.date()
    gps_zamani = utc_time.time()

    return gps_tarihi, gps_zamani



def max_gps_per_row(tab_data):
    """
    Her satırdaki GPS sütunlarının en yüksek değerini alır.

    Args:
        tab_data (pd.DataFrame): Veri.

    Returns:
        pd.Series: Her satırdaki en yüksek GPS_Milliseconds değeri.
    """
    max_values = tab_data.max(axis=1)  # Satır bazında maksimum değerleri al
    return max_values



def normalize_time(time_str):

    if isinstance(time_str, time):
        time_str = time_str.strftime("%H:%M:%S.%f")[:-3]

    try:
        parts = time_str.split(":")

        hours   = int(parts[0])
        minutes = int(parts[1])
        seconds = float(parts[2])

        if seconds >= 60:
            extra_minutes = int(seconds // 60)
            seconds      %= 60
            hours        += extra_minutes


        if minutes >= 60:
            extra_hours   = int(minutes // 60)
            seconds      %= 60
            hours        += extra_hours

        if hours >= 24:
            hours %= 24

        
        return f"{hours:02}:{minutes:02}:{seconds:06.3f}"

    except Exception as e:
        print(f"Hata: '{e}', Zaman Degeri: '{time_str}'")



def time_to_seconds(time_str):

    parts = str(time_str).strip().split(":")
    
    if len(parts) == 2:
        
        hours = 0
        minutes = int(parts[0]) * 60
        seconds = float(parts[1])
        
        return minutes + seconds
    
    elif len(parts) == 3:  # GPS zamanı formatı (hh:mm:ss.d)
        
        hours = int(parts[0]) * 3600
        minutes = int(parts[1]) * 60
        seconds = float(parts[2])
        
        return hours + minutes + seconds
    else:
        raise ValueError(f"GPS zamanı formatı yanlış: '{time_str}'")


# Saniyeyi zaman formatına çevirme fonksiyonu

def seconds_to_time(seconds):
    
    hours    = int(seconds // 3600)
    seconds %= 3600
    minutes  = int(seconds // 60)
    seconds %= 60
    
    return f"{hours:02}:{minutes:02}:{seconds:06.3f}"



def interpolate_and_fix_time(data):
    """
    Verilen zaman sütunu üzerinde hatalı düşüş yapan değerleri tespit eder ve interpolasyon uygular.
    tab_data'lar arasında artan sıralamayı korur.

    Args:
        data (pd.DataFrame): Veri çerçevesi.
        time_column (str): Zaman sütununun adı.
        last_valid_value (float): Bir önceki tab_data'tan gelen son doğru değer.

    Returns:
        pd.DataFrame, float: Güncellenmiş veri çerçevesi ve son geçerli değer.
    """
    data["Port_1_Makine_Zamani_Sec"] = data["Port 1 Makine Zamani"].apply(lambda x: time_to_seconds(x))

    time_column = "Port_1_Makine_Zamani_Sec"


    # Hatalı düşüşleri tespit et
    sorted_indices = data[data[time_column].diff().lt(0)].index

    for idx in sorted_indices:
        # Eğer idx geçersizse bir önceki doğru değeri bul
        start_idx = idx - 1

        while start_idx > 0:
            # Artan bir bölge tespit edilirse dur
            if data.loc[start_idx, time_column] >= data.loc[start_idx - 1, time_column]:
                break
            
            start_idx -= 1

        end_idx = idx
        
        while end_idx < len(data) - 1 and data.loc[end_idx, time_column] < data.loc[start_idx, time_column]:
            end_idx += 1

        # Eğer end_idx start_idx'ye eşitse, geçerli bir interpolasyon yapılmaz.
        if start_idx == end_idx:
            continue
 
        start_value = data.loc[start_idx, time_column]
        end_value   = data.loc[end_idx, time_column]

        # Hatalı aralık
        incorrect_range = list(range(start_idx + 1, end_idx))

        # Eğer yalnızca bir değer varsa, doğrudan iki değer arasındaki ortalama alınır
        if len(incorrect_range) == 1:
            data.loc[incorrect_range[0], time_column] = (start_value + end_value) / 2
        else:
            # Interpolasyon
            for i, j in enumerate(incorrect_range):
                data.loc[j, time_column] = start_value + (end_value - start_value) * (i + 1) / len(incorrect_range)

    # Eşit değerlerin tespiti ve artırılması
    for i in range(1, len(data)):
        if data.loc[i, time_column] <= data.loc[i - 1, time_column]:
            data.loc[i, time_column] = data.loc[i - 1, time_column] + 0.001

    
    data["Port 1 Makine Zamani"] = data["Port_1_Makine_Zamani_Sec"].apply(seconds_to_time)
    data["Port 1 Makine Zamani"] = data["Port 1 Makine Zamani"].apply(normalize_time)

    data.drop(columns = ["Port_1_Makine_Zamani_Sec"], inplace = True, errors ="ignore")



def gps_process(tab_data):
    tab_data["Max_GPS_Zamani"] = max_gps_per_row(tab_data[["surek_1","surek_2","surek_3"]])

    utc_results = tab_data.apply(lambda row: gps_zamani_hesapla(row['GPS_Week'], row['GPS_Milliseconds']),axis=1)
    
    gps_tarih, gps_zaman= zip(*utc_results)

        # 8. ve 9. sütunlara ekle
    tab_data.insert(8, 'GPS_Tarih', gps_tarih)  # 8. sütun (indeks 7)
    tab_data.insert(9, 'GPS_Saat', gps_zaman)   # 9. sütun (indeks 8)


        # Zamanları saniyeye çevir
    tab_data["GPS_Zamani_Sec"] = tab_data["GPS_Zamani"].apply(time_to_seconds)
    tab_data["Port_1_Makine_Zamani_Sec"] = tab_data["Port 1 Makine Zamani"].apply(time_to_seconds)
    
    tab_data["GPS_Tarihi"] = tab_data["GPS_Tarihi"].astype(str).str.strip()
    
    subset_tab_data_1980 = tab_data[tab_data["GPS_Tarihi"] == "1980-01-05"].copy()

    subset_tab_data_1980["Yeni_GPS_Zamani_Sec"] = subset_tab_data_1980["GPS_Zamani_Sec"]

    for i in range(1, len(subset_tab_data_1980)):
        makine_delta = subset_tab_data_1980.iloc[i]["Makine_Zamani_Sec"] - subset_tab_data_1980.iloc[i - 1]["Makine_Zamani_Sec"]
        if subset_tab_data_1980.iloc[i]["GPS_Zamani_Sec"] <= subset_tab_data_1980.iloc[i - 1]["Yeni_GPS_Zamani_Sec"]:
            subset_tab_data_1980.loc[subset_tab_data_1980.index[i], "Yeni_GPS_Zamani_Sec"] = subset_tab_data_1980.iloc[i - 1]["Yeni_GPS_Zamani_Sec"] + makine_delta

    # Diğer tarihleri eşitleme
    subset_others = tab_data[tab_data["GPS_Tarihi"] != "1980-01-05"].copy()
    subset_others["Yeni_GPS_Zamani_Sec"] = subset_others["GPS_Zamani_Sec"]

    for i in range(1, len(subset_others)):
        makine_delta = subset_others.iloc[i]["Makine_Zamani_Sec"] - subset_others.iloc[i - 1]["Makine_Zamani_Sec"]
        if subset_others.iloc[i]["GPS_Zamani_Sec"] <= subset_others.iloc[i - 1]["Yeni_GPS_Zamani_Sec"]:
            subset_others.loc[subset_others.index[i], "Yeni_GPS_Zamani_Sec"] = subset_others.iloc[i - 1]["Yeni_GPS_Zamani_Sec"] + makine_delta

    # Orijinal veriyle birleştir
    tab_data["Yeni_GPS_Zamani_Sec"] = tab_data["GPS_Zamani_Sec"]  # Başlangıçta kopyalama
    tab_data.loc[subset_tab_data_1980.index, "Yeni_GPS_Zamani_Sec"] = subset_tab_data_1980["Yeni_GPS_Zamani_Sec"]
    tab_data.loc[subset_others.index, "Yeni_GPS_Zamani_Sec"] = subset_others["Yeni_GPS_Zamani_Sec"]

    # Zamanı tekrar orijinal formatına çevir
    tab_data["Yeni_GPS_Zamani"] = tab_data["Yeni_GPS_Zamani_Sec"].apply(seconds_to_time)
    tab_data["Makine_Zamani"] = tab_data["Makine_Zamani_Sec"].apply(seconds_to_time)
    tab_data["GPS_Zamani"] = tab_data["Yeni_GPS_Zamani"]

    # Gereksiz sütunları temizle
    tab_data.drop(columns=["Yeni_GPS_Zamani_Sec", "Yeni_GPS_Zamani"], inplace=True, errors="ignore")

    # Sonuçları görüntüleme
    print(tab_data[["GPS_Tarihi", "GPS_Zamani", "Makine_Zamani"]])



