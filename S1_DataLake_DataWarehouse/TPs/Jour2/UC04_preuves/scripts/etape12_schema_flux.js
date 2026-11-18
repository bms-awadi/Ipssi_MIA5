// Schema du flux NiFi enrichi (UC-04 etape 12)
const { canvas } = require('./diag_lib');
const out = process.argv[2];
const W = 1200, H = 1330, d = canvas(W, H);
const CX = 600;

d.box(400, 20, 400, 80, '/data/nifi/input', ['dossier local surveillé'], { dash: true });
d.arrow(CX, 100, CX, 148);
d.box(380, 150, 440, 100, 'GetFile', ['lit chaque fichier, puis le retire du dossier', 'toutes les 5 s'], { head: true });
d.arrow(CX, 250, CX, 298); d.text(CX + 12, 280, 'success', { anchor: 'start', italic: true, size: 16 });
d.box(330, 300, 540, 190, 'UpdateAttribute', [], { head: true });
['source = tpcxbb', 'lot = initial', 'table = nom du fichier sans extension', 'date_ingestion = date et heure de passage', 'flux = UC04-ingestion-enrichie']
  .forEach((t, i) => d.text(355, 372 + i * 26, t, { anchor: 'start', size: 17 }));
d.arrow(CX, 490, CX, 538); d.text(CX + 12, 520, 'success', { anchor: 'start', italic: true, size: 16 });
d.box(330, 540, 540, 110, 'RouteOnAttribute', ['dat = le nom se termine par .dat', 'sinon : unmatched'], { head: true });

// branches
d.line(470, 650, 470, 690); d.line(250, 690, 470, 690); d.arrow(250, 690, 250, 748);
d.text(260, 725, 'dat', { anchor: 'start', italic: true, size: 17, bold: true });
d.line(730, 650, 730, 690); d.line(730, 690, 950, 690); d.arrow(950, 690, 950, 748);
d.text(960, 725, 'unmatched', { anchor: 'start', italic: true, size: 17, bold: true });

d.box(60, 750, 380, 110, 'PutHDFS', ['n\'écrase jamais (fail)', 'un dossier par table'], { head: true });
d.box(760, 750, 380, 110, 'PutFile', ['n\'écrase jamais (fail)', 'mise de côté locale'], { head: true });
d.arrow(250, 860, 250, 918); d.arrow(950, 860, 950, 918);
d.box(60, 920, 380, 80, 'HDFS', ['/retail-lake/raw/nifi/<table>/'], { fill: '#F2F2F2' });
d.box(760, 920, 380, 80, 'Rejets locaux', ['/data/nifi/error/'], { fill: '#F2F2F2' });

// vers LogAttribute
d.line(440, 805, 520, 805); d.arrow(520, 805, 520, 1028);
d.text(452, 795, 'success, failure', { anchor: 'start', italic: true, size: 15 });
d.line(760, 805, 680, 805); d.arrow(680, 805, 680, 1028);
d.text(748, 795, 'success, failure', { anchor: 'end', italic: true, size: 15 });
d.box(400, 1030, 400, 100, 'LogAttribute', ['écrit les attributs de chaque', 'FlowFile dans le journal'], { head: true });
d.arrow(CX, 1130, CX, 1168);
d.box(400, 1170, 400, 60, '', ['journal nifi-app.log'], { dash: true });

d.text(40, 1275, 'Test du 17/11/2026 : reason.dat et ship_mode.dat déposés dans HDFS, item_marketprices_audit.csv mis en rejet,', { anchor: 'start', size: 16 });
d.text(40, 1300, 'les 3 FlowFiles journalisés par LogAttribute. Aucun fichier existant n\'a été écrasé.', { anchor: 'start', size: 16 });
d.save(out);
