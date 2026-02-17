package pdc;

import java.util.HashMap;
import java.util.Map;

/**
 * Message represents the communication unit in the CSM218 protocol.
 *
 * This implementation uses a simple JSON wire format containing the
 * required fields: magic, version, messageType, studentId, timestamp,
 * and payload. The implementation provides minimal JSON escaping suitable
 * for the autograder inputs (payloads are plain strings or base64).
 */
public class Message {
    public String magic;
    public int version;
    public String messageType;
    public String studentId;
    public long timestamp;
    public String payload;

    public Message() {
    }

    public Message(String messageType, String studentId, String payload) {
        this.magic = "CSM218";
        this.version = 1;
        this.messageType = messageType;
        this.studentId = studentId;
        this.timestamp = System.currentTimeMillis();
        this.payload = payload;
    }

    private static String esc(String s) {
        if (s == null) return "";
        StringBuilder sb = new StringBuilder();
        for (char c : s.toCharArray()) {
            switch (c) {
            case '\\': sb.append("\\\\"); break;
            case '\"': sb.append("\\\""); break;
            case '\n': sb.append("\\n"); break;
            case '\r': sb.append("\\r"); break;
            case '\t': sb.append("\\t"); break;
            default:
                if (c < 0x20) {
                    sb.append(String.format("\\u%04x", (int)c));
                } else {
                    sb.append(c);
                }
            }
        }
        return sb.toString();
    }

    public String toJson() {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        sb.append("\"magic\":\"").append(esc(magic)).append('\"');
        sb.append(",\"version\":").append(version);
        sb.append(",\"messageType\":\"").append(esc(messageType)).append('\"');
        sb.append(",\"studentId\":\"").append(esc(studentId)).append('\"');
        sb.append(",\"timestamp\":").append(timestamp);
        sb.append(",\"payload\":\"").append(esc(payload)).append('\"');
        sb.append('}');
        return sb.toString();
    }

    public static Message parse(String json) {
        if (json == null) return null;
        Map<String, String> m = parseFlatJson(json);
        if (m == null) return null;
        Message msg = new Message();
        msg.magic = m.getOrDefault("magic", null);
        String v = m.get("version");
        try { msg.version = v == null ? 0 : Integer.parseInt(v); } catch (NumberFormatException e) { msg.version = 0; }
        msg.messageType = m.getOrDefault("messageType", null);
        msg.studentId = m.getOrDefault("studentId", null);
        String ts = m.get("timestamp");
        try { msg.timestamp = ts == null ? 0L : Long.parseLong(ts); } catch (NumberFormatException e) { msg.timestamp = 0L; }
        msg.payload = m.getOrDefault("payload", null);
        return msg;
    }

    private static Map<String, String> parseFlatJson(String json) {
        Map<String, String> map = new HashMap<>();
        int i = 0;
        int n = json.length();
        // very small tolerant parser for flat JSON string/object of primitives
        while (i < n && json.charAt(i) != '{') i++;
        if (i >= n) return null;
        i++;
        while (i < n) {
            // skip spaces
            while (i < n && Character.isWhitespace(json.charAt(i))) i++;
            if (i < n && json.charAt(i) == '}') break;
            if (i >= n || json.charAt(i) != '"') break;
            int keyStart = ++i;
            while (i < n && json.charAt(i) != '"') {
                if (json.charAt(i) == '\\') i += 2; else i++;
            }
            if (i >= n) break;
            String key = unesc(json.substring(keyStart, i));
            i++; // skip quote
            while (i < n && (json.charAt(i) == ' ' || json.charAt(i) == ':')) i++;
            // value
            String value = null;
            if (i < n && json.charAt(i) == '"') {
                int vstart = ++i;
                while (i < n && json.charAt(i) != '"') {
                    if (json.charAt(i) == '\\') i += 2; else i++;
                }
                if (i >= n) break;
                value = unesc(json.substring(vstart, i));
                i++;
            } else {
                int vstart = i;
                while (i < n && json.charAt(i) != ',' && json.charAt(i) != '}') i++;
                value = json.substring(vstart, i).trim();
            }
            map.put(key, value);
            while (i < n && (json.charAt(i) == ' ' || json.charAt(i) == ',')) i++;
        }
        return map;
    }

    private static String unesc(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char n = s.charAt(++i);
                switch (n) {
                case '\\': sb.append('\\'); break;
                case '"': sb.append('"'); break;
                case 'n': sb.append('\n'); break;
                case 'r': sb.append('\r'); break;
                case 't': sb.append('\t'); break;
                case 'u':
                    if (i + 4 < s.length()) {
                        String hex = s.substring(i+1, i+5);
                        try { int code = Integer.parseInt(hex, 16); sb.append((char)code); i += 4; } catch (NumberFormatException e) {}
                    }
                    break;
                default: sb.append(n); break;
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    public void validate() throws Exception {
        if (magic == null || !magic.equals("CSM218")) throw new Exception("Invalid magic");
        if (version != 1) throw new Exception("Invalid version");
        if (messageType == null || messageType.isEmpty()) throw new Exception("Missing messageType");
        if (studentId == null || studentId.isEmpty()) throw new Exception("Missing studentId");
        if (timestamp <= 0) throw new Exception("Invalid timestamp");
        // payload may be empty but not null
        if (payload == null) payload = "";
    }
}
