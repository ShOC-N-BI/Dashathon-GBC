#start 

import re
from collections import Counter

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
    friendly_callsign = friendly["callsign"]
    action = friendly["matched_actions"]
    target_callsign = extract_target_name(target)

    return f"{friendly_callsign} {action} {target_callsign}"

def line_two():
    return

def line_three():
    return

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
        parts.append(f"{num} enemy {h_type} {word}")

    if not parts:
        return "No enemy targets detected."

    return " and ".join(parts) + " possibly enroute."

def line_five():
    return




def generate(armament, hostiles, fuel, time, support, sequence, message, friendly, target):
    # line 1 = message
    line_uno = line_one(friendly, target)
    # line 2 = support
    # line 3 = sequence
    # line 4 = hostiles
    line_cuatro = line_four(hostiles)
    # line 5 = expanded support

    final_five_line = f"LINE 1: {line_uno}\nLINE 4: {line_cuatro}"
    

    return final_five_line