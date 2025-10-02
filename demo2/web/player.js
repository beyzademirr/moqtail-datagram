// MOQT V9 Demo Player - Parses Draft-11 wire format from your DatagramHandlerV9

const logEl = document.getElementById('log');
const statusEl = document.getElementById('status');
const canvas = document.getElementById('screen');
const ctx = canvas.getContext('2d', { alpha: false });
const frameInfoEl = document.getElementById('frame-info');

// Stats elements
const objectCountEl = document.getElementById('object-count');
const bytesCountEl = document.getElementById('bytes-count');
const currentTrackEl = document.getElementById('current-track');
const lastGroupEl = document.getElementById('last-group');
const lastObjectEl = document.getElementById('last-object');
const extCountEl = document.getElementById('ext-count');

let stats = {
  objectCount: 0,
  bytesCount: 0,
  currentTrack: null,
  lastGroup: null,
  lastObject: null,
  extCount: 0
};

document.getElementById('connect').onclick = async () => {
  try {
    setStatus('Connecting…', 'connecting');
    clearLog();
    log('🔗 Connecting to MOQT V9 server...');
    
    // Connect via WebTransport (requires HTTPS context or localhost exception)
    const wt = new WebTransport('https://localhost:4433');
    await wt.ready;
    
    setStatus('Connected', 'connected');
    log('✅ WebTransport connection established');
    log('📡 Waiting for MOQT datagrams...');

    // Read datagrams and parse V9 wire format
    const reader = wt.datagrams.readable.getReader();
    for (;;) {
      const { value, done } = await reader.read();
      if (done) {
        log('📪 Connection closed by server');
        break;
      }
      if (!value) continue;
      
      try {
        await handleDatagram(value);
      } catch (e) {
        log(`❌ Parse error: ${e.message}`);
        console.error('Datagram parse error:', e, value);
      }
    }
  } catch (e) {
    setStatus('Failed', 'error');
    log(`❌ Connection failed: ${e.message}`);
    console.error('WebTransport error:', e);
  }
};

function setStatus(text, className) {
  statusEl.textContent = text;
  statusEl.className = `status-${className}`;
}

function log(s) { 
  const timestamp = new Date().toLocaleTimeString();
  logEl.textContent += `[${timestamp}] ${s}\n`;
  logEl.scrollTop = logEl.scrollHeight;
}

function clearLog() {
  logEl.textContent = '';
}

function updateStats() {
  objectCountEl.textContent = stats.objectCount;
  bytesCountEl.textContent = formatBytes(stats.bytesCount);
  currentTrackEl.textContent = stats.currentTrack ?? '-';
  lastGroupEl.textContent = stats.lastGroup ?? '-';
  lastObjectEl.textContent = stats.lastObject ?? '-';
  extCountEl.textContent = stats.extCount;
}

function formatBytes(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return Math.round(bytes / 1024) + ' KB';
  return Math.round(bytes / (1024 * 1024)) + ' MB';
}

// ---- QUIC Varint parsing (matches your Rust implementation) ----
function readVarint(view, offset) {
  if (offset >= view.byteLength) {
    throw new Error('Varint read beyond buffer');
  }
  
  const first = view.getUint8(offset);
  let len, val;
  
  if ((first & 0b11000000) === 0b00000000) {
    // 1 byte: 00xxxxxx
    len = 1;
    val = first & 0b00111111;
  } else if ((first & 0b11000000) === 0b01000000) {
    // 2 bytes: 01xxxxxx xxxxxxxx
    if (offset + 1 >= view.byteLength) throw new Error('Varint 2-byte read beyond buffer');
    len = 2;
    val = ((first & 0b00111111) << 8) | view.getUint8(offset + 1);
  } else if ((first & 0b11000000) === 0b10000000) {
    // 4 bytes: 10xxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
    if (offset + 3 >= view.byteLength) throw new Error('Varint 4-byte read beyond buffer');
    len = 4;
    val = ((first & 0b00111111) << 24) |
          (view.getUint8(offset + 1) << 16) |
          (view.getUint8(offset + 2) << 8) |
          view.getUint8(offset + 3);
  } else {
    // 8 bytes: 11xxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
    if (offset + 7 >= view.byteLength) throw new Error('Varint 8-byte read beyond buffer');
    len = 8;
    // JavaScript safe integer handling for 64-bit values
    const hi = ((first & 0b00111111) * Math.pow(2, 32)) +
               (view.getUint8(offset + 1) << 24) +
               (view.getUint8(offset + 2) << 16) +
               (view.getUint8(offset + 3) << 8) +
               view.getUint8(offset + 4);
    const lo = (view.getUint8(offset + 5) << 16) +
               (view.getUint8(offset + 6) << 8) +
               view.getUint8(offset + 7);
    val = hi * Math.pow(2, 24) + lo;
  }
  
  return { val, len };
}

// MOQT Datagram Types (from your V9 handler)
const DATAGRAM_TYPE = {
  OBJECT: 0x00,
  OBJECT_WITH_EXT: 0x01,
  STATUS: 0x02,
  STATUS_WITH_EXT: 0x03,
};

// Extension Types
const EXT_TYPE = {
  SUBGROUP_ID: 0x04,
};

// Enhanced Status Codes (V9 feature)
const STATUS_CODE = {
  NORMAL: 0x00,
  DOES_NOT_EXIST: 0x01,
  GROUP_DOES_NOT_EXIST: 0x02,
  END_OF_GROUP: 0x03,
  END_OF_TRACK: 0x04,
  END_OF_SUBGROUP: 0x05,
};

// Parse MOQT V9 datagram according to Draft-11 wire format
async function handleDatagram(uint8Array) {
  const view = new DataView(uint8Array.buffer, uint8Array.byteOffset, uint8Array.byteLength);
  let pos = 0;

  // Parse datagram type
  const typeResult = readVarint(view, pos);
  const datagramType = typeResult.val;
  pos += typeResult.len;

  stats.bytesCount += uint8Array.length;

  if (datagramType === DATAGRAM_TYPE.OBJECT || datagramType === DATAGRAM_TYPE.OBJECT_WITH_EXT) {
    await parseObjectDatagram(view, pos, datagramType, uint8Array);
  } else if (datagramType === DATAGRAM_TYPE.STATUS || datagramType === DATAGRAM_TYPE.STATUS_WITH_EXT) {
    parseStatusDatagram(view, pos, datagramType);
  } else {
    throw new Error(`Unknown datagram type: 0x${datagramType.toString(16)}`);
  }
  
  updateStats();
}

async function parseObjectDatagram(view, pos, datagramType, fullArray) {
  // Parse Track Alias
  const trackResult = readVarint(view, pos);
  const trackAlias = trackResult.val;
  pos += trackResult.len;

  // Parse Group ID
  const groupResult = readVarint(view, pos);
  const groupId = groupResult.val;
  pos += groupResult.len;

  // Parse Object ID
  const objectResult = readVarint(view, pos);
  const objectId = objectResult.val;
  pos += objectResult.len;

  // Parse Publisher Priority (u8)
  if (pos >= view.byteLength) throw new Error('Missing publisher priority');
  const priority = view.getUint8(pos);
  pos += 1;

  let subgroupId = null;
  let extensionCount = 0;

  // Parse Extensions (V9 feature)
  if (datagramType === DATAGRAM_TYPE.OBJECT_WITH_EXT) {
    const extTotalResult = readVarint(view, pos);
    const extTotalLen = extTotalResult.val;
    pos += extTotalResult.len;

    const extStartPos = pos;
    const extEndPos = pos + extTotalLen;

    if (extEndPos > view.byteLength) {
      throw new Error('Extension length exceeds datagram size');
    }

    // Parse individual extensions
    while (pos < extEndPos) {
      const extTypeResult = readVarint(view, pos);
      const extType = extTypeResult.val;
      pos += extTypeResult.len;

      const extLenResult = readVarint(view, pos);
      const extLen = extLenResult.val;
      pos += extLenResult.len;

      if (pos + extLen > extEndPos) {
        throw new Error('Extension data length exceeds total extension length');
      }

      // Handle known extensions
      if (extType === EXT_TYPE.SUBGROUP_ID) {
        const subgroupView = new DataView(view.buffer, view.byteOffset + pos, extLen);
        const subgroupResult = readVarint(subgroupView, 0);
        subgroupId = subgroupResult.val;
      }

      pos += extLen;
      extensionCount++;
    }

    stats.extCount += extensionCount;
  }

  // Remaining bytes are the payload
  const payload = fullArray.slice(pos);

  // Update stats
  stats.objectCount++;
  stats.currentTrack = trackAlias;
  stats.lastGroup = groupId;
  stats.lastObject = objectId;

  // Log the received object
  const extInfo = extensionCount > 0 ? ` ext=${extensionCount}` : '';
  const subInfo = subgroupId !== null ? ` sub=${subgroupId}` : '';
  log(`📦 Object t=${trackAlias} g=${groupId} o=${objectId} prio=${priority}${subInfo}${extInfo} size=${payload.length}B`);

  // Update frame info
  frameInfoEl.innerHTML = `
    <strong>Latest Frame:</strong><br>
    Track: ${trackAlias} | Group: ${groupId} | Object: ${objectId}<br>
    Priority: ${priority} | Subgroup: ${subgroupId ?? 'None'}<br>
    Extensions: ${extensionCount} | Size: ${formatBytes(payload.length)}
  `;

  // Render the "frame" (simulate video frame)
  await renderFrame(payload, trackAlias, groupId, objectId, subgroupId);
}

function parseStatusDatagram(view, pos, datagramType) {
  // Parse Track Alias
  const trackResult = readVarint(view, pos);
  const trackAlias = trackResult.val;
  pos += trackResult.len;

  // Parse Group ID
  const groupResult = readVarint(view, pos);
  const groupId = groupResult.val;
  pos += groupResult.len;

  // Parse Object ID
  const objectResult = readVarint(view, pos);
  const objectId = objectResult.val;
  pos += objectResult.len;

  // Parse Status Code
  const statusResult = readVarint(view, pos);
  const statusCode = statusResult.val;
  pos += statusResult.len;

  // Skip extensions if present (not parsed in this demo)
  if (datagramType === DATAGRAM_TYPE.STATUS_WITH_EXT) {
    const extTotalResult = readVarint(view, pos);
    pos += extTotalResult.len + extTotalResult.val;
  }

  // Map status code to readable name
  const statusName = Object.entries(STATUS_CODE).find(([_, code]) => code === statusCode)?.[0] || `0x${statusCode.toString(16)}`;

  log(`📊 Status t=${trackAlias} g=${groupId} o=${objectId} status=${statusName}`);

  if (statusCode === STATUS_CODE.END_OF_GROUP) {
    log('🏁 End of group reached');
  } else if (statusCode === STATUS_CODE.END_OF_TRACK) {
    log('🏁 End of track reached');
  }
}

async function renderFrame(payload, trackAlias, groupId, objectId, subgroupId) {
  // Clear canvas
  ctx.fillStyle = '#000';
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  // Check if this is our demo frame format
  if (payload.length >= 13) {
    const header = new TextDecoder().decode(payload.slice(0, 10));
    if (header === 'DEMO_FRAME') {
      const r = payload[10];
      const g = payload[11];
      const b = payload[12];

      // Draw colored rectangle representing the "frame"
      ctx.fillStyle = `rgb(${r}, ${g}, ${b})`;
      ctx.fillRect(50, 50, canvas.width - 100, canvas.height - 100);

      // Add frame info overlay
      ctx.fillStyle = 'white';
      ctx.font = '16px system-ui';
      ctx.fillText(`Frame ${objectId}`, 60, 80);
      ctx.font = '12px system-ui';
      ctx.fillText(`RGB(${r}, ${g}, ${b})`, 60, 100);
      ctx.fillText(`Track ${trackAlias}, Group ${groupId}`, 60, 120);
      if (subgroupId !== null) {
        ctx.fillText(`Subgroup ${subgroupId}`, 60, 140);
      }

      log(`🎨 Rendered ${header} frame: RGB(${r}, ${g}, ${b})`);
    } else {
      // Unknown format, just show some info
      ctx.fillStyle = '#333';
      ctx.fillRect(50, 50, canvas.width - 100, canvas.height - 100);
      ctx.fillStyle = 'white';
      ctx.font = '14px system-ui';
      ctx.fillText(`Object ${objectId}`, 60, 80);
      ctx.fillText(`${payload.length} bytes`, 60, 100);
    }
  } else {
    // Too small, show placeholder
    ctx.fillStyle = '#333';
    ctx.fillRect(50, 50, canvas.width - 100, canvas.height - 100);
    ctx.fillStyle = 'white';
    ctx.font = '14px system-ui';
    ctx.fillText('Small payload', 60, 80);
    ctx.fillText(`${payload.length} bytes`, 60, 100);
  }
}