#start 

import re
from collections import Counter
import random

cyber_ids = ["Cereal", "Condor", "Light", "Wolf"]
space_ids = ["Photon", "Astro", "Pluto", "Vader", "JarJar", "Roo"]

def extract_target_name(line: str) -> str | None:
    """extract target callsign or tracknumber if no callsign available"""

    if not line or not isinstance(line, str):
        return None

    # 1) Try to find a CallSign value (case-insensitive)
    cs_match = re.search(r'CallSign\s*:\s*([^,)\n]+)', line, flags=re.IGNORECASE)
    if cs_match:
        callsign = cs_match.group(1).strip()
        # treat "None" or empty as absent
        if callsign and callsign.lower() != "none":
            return callsign

    # 2) Try to capture a 5-digit track number at the beginning
    m_start5 = re.match(r'^\s*(\d{5})\b', line)
    if m_start5:
        return m_start5.group(1)

    # 3) If the line begins with a longer run of digits, take the first 5 digits
    m_start_digits = re.match(r'^\s*(\d+)', line)
    if m_start_digits:
        digits = m_start_digits.group(1)
        if len(digits) >= 5:
            return digits[:5]

    # 4) Otherwise, search for a standalone 5-digit number anywhere (e.g., "..., 44920, ...")
    m_any5 = re.search(r'\b(\d{5})\b', line)
    if m_any5:
        return m_any5.group(1)

    # nothing found
    return None





def line_one(friendly, target):
    """Builds out the first line in the five line using the friendly and target names with the matched actions."""

    # Use callsign if available, otherwise fallback to tracknumber
    friendly_callsign = friendly.get("callsign") or str(friendly.get("merged_tracknumber", "UNKNOWN"))

    action = friendly["matched_actions"][0]
    target_callsign = extract_target_name(target)

    return f"{friendly_callsign} {action} {target_callsign}"

def line_two(support):
    """produces all supporting assets"""
    support_list = []

    for role, details in support.items():
        if isinstance(details, dict):
            callsign = details.get("callsign")
            tracknumber = details.get("tracknumber")

            #looks for callsign, if no callsign then None
            if callsign and str(callsign).strip() != "None":
                support_list.append(callsign.strip())
            elif tracknumber and str(tracknumber).strip() != "None":
                support_list.append(str(tracknumber).strip())

    supporting_assets = f"{", ".join(support_list)}"

    return supporting_assets

def line_three(sequence):
    return sequence

def line_four(hostiles):
    """Takes the number and types of hostiles to create the fourth line"""
    #hostiles = (4, [(45251, 'Pending', 'Land'), (45212, 'Pending', 'Land'), (45196, 'Pending', 'Air')])
    hostile_list = hostiles[1]
    hostile_types = [entry[2] for entry in hostile_list]
    counts = Counter(hostile_types)
    # build message parts
    parts = []
    for h_type, num in counts.items():
        word = "target" if num == 1 else "targets"
        parts.append(f"{num} confirmed hostile(s) {h_type} {word}")

    if not parts:
        return "No hostile targets detected en route."

    return " and ".join(parts) + " possibly enroute."

def line_five(support):
    rand_cyber = random.choice(cyber_ids)
    rand_space = random.choice(space_ids)
    line = "CYBER: " + rand_cyber + ", SPACE: " + rand_space 

    escort_list = support["escort"]
    tanker_list = support["tankers"]
    awacs_list = support["awacs"]
    ew_list = support["ew"]
    sead_list = support["sead"]

    print(f"AWACS -- {awacs_list}")

    # Extract callsign if available, else fallback to bc3_jtn
    
    if isinstance(escort_list, list):
        escort_names = [
            str(e["callsign"]) if e["callsign"] is not None else str(e["tracknumber"])
            for e in escort_list
        ]
        print("Escort names found")
        # Convert to single string
        escort_string = ", ".join(escort_names)
        line = line + ", ESCORT: " + escort_string
        #print(f"ESCORTS: {escort_string}")
    
        # print("----escort not list----")

    try:
        awacs_string = awacs_list["callsign"] or awacs_list["tracknumber"]
        print(awacs_string)
        print(type(awacs_string))
        line = line + ", AWACS: " + str(awacs_string)
    except: 
        print("no awacs")

    try: 
        sead_string = sead_list["callsign"] or sead_list["tracknumber"]
        line = line + ", SEAD: " + str(sead_string)
    except:
        print("No sead")

    try:
        ew_string = ew_list["callsign"] or ew_list["tracknumber"]
        line = line + ", EW: " + str(ew_string)
    except: 
        print("no ew")

    

    return line




def generate(armament, hostiles, fuel, time, support, sequence, message, friendly, target):
    
    line_uno = line_one(friendly, target) # line 1 = message
    line_dos = line_two(support) # line 2 = support
    line_tres = line_three(sequence) # line 3 = sequence
    # print(f"**********************{sequence}")
    line_cuatro = line_four(hostiles) # line 4 = hostiles
    line_cinco = line_five(support) # line 5 = expanded support
    
    coa = {
        "FirstLine": f"{line_uno}",
        "SecondLine": f"{line_dos}",
        "ThirdLine": f"{line_tres}",
        "FourthLine": f"{line_cuatro}",
        "FifthLine": f"{line_cinco}"
    }

    return coa