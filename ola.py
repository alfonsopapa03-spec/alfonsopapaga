<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Formularios — Mi Empresa</title>
  <link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:wght@300;400;500;600&display=swap" rel="stylesheet"/>
  <style>
    *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
    :root{
      --blue:#1A56A0;--blue-light:#E8F0FB;--blue-mid:#3B7DD8;
      --green:#256B35;--green-light:#E6F4EA;--green-mid:#3A9152;
      --gray-50:#F7F6F3;--gray-100:#EDEBE6;--gray-300:#C5C2BA;
      --gray-500:#7D7A72;--gray-700:#3D3B36;--gray-900:#18170F;
      --white:#FFFFFF;--radius-sm:6px;--radius-md:12px;--radius-lg:20px;
      --shadow-sm:0 1px 3px rgba(0,0,0,0.08);--shadow-md:0 4px 20px rgba(0,0,0,0.10);--shadow-lg:0 12px 48px rgba(0,0,0,0.14);
    }
    html{scroll-behavior:smooth}
    body{font-family:'DM Sans',sans-serif;background:var(--gray-50);color:var(--gray-900);min-height:100vh}
    nav{position:sticky;top:0;z-index:100;background:rgba(247,246,243,0.92);backdrop-filter:blur(12px);border-bottom:1px solid var(--gray-100);padding:0 2rem;height:60px;display:flex;align-items:center;justify-content:space-between}
    .nav-logo{font-family:'DM Serif Display',serif;font-size:20px;color:var(--gray-900)}
    .nav-logo span{color:var(--blue)}
    .nav-link{font-size:13px;font-weight:500;color:var(--gray-500);text-decoration:none;transition:color 0.2s}
    .nav-link:hover{color:var(--gray-900)}
    .hero{padding:72px 2rem 56px;text-align:center;position:relative;overflow:hidden}
    .hero::before{content:'';position:absolute;inset:0;background:radial-gradient(ellipse 600px 300px at 20% 50%,rgba(26,86,160,0.06) 0%,transparent 70%),radial-gradient(ellipse 500px 250px at 80% 50%,rgba(37,107,53,0.05) 0%,transparent 70%);pointer-events:none}
    .hero-eyebrow{display:inline-flex;align-items:center;gap:6px;background:var(--white);border:1px solid var(--gray-100);border-radius:100px;padding:5px 14px;font-size:12px;font-weight:500;color:var(--gray-500);margin-bottom:20px;box-shadow:var(--shadow-sm)}
    .hero-eyebrow .dot{width:6px;height:6px;border-radius:50%;background:var(--green-mid);animation:pulse 2s infinite}
    @keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:0.5;transform:scale(0.8)}}
    .hero h1{font-family:'DM Serif Display',serif;font-size:clamp(32px,5vw,52px);font-weight:400;color:var(--gray-900);letter-spacing:-1px;line-height:1.15;margin-bottom:16px}
    .hero h1 em{font-style:italic;color:var(--blue)}
    .hero p{font-size:16px;color:var(--gray-500);max-width:480px;margin:0 auto;line-height:1.7}
    .cards-section{max-width:900px;margin:0 auto;padding:0 2rem 56px;display:grid;grid-template-columns:1fr 1fr;gap:20px}
    .form-card{background:var(--white);border:1.5px solid var(--gray-100);border-radius:var(--radius-lg);padding:2rem;cursor:pointer;transition:border-color 0.25s,transform 0.2s,box-shadow 0.25s;position:relative;overflow:hidden}
    .form-card::after{content:'';position:absolute;bottom:0;left:0;right:0;height:3px;border-radius:0 0 var(--radius-lg) var(--radius-lg);opacity:0;transition:opacity 0.25s}
    .form-card.cliente::after{background:var(--blue-mid)}
    .form-card.proveedor::after{background:var(--green-mid)}
    .form-card:hover{transform:translateY(-2px);box-shadow:var(--shadow-md)}
    .form-card.active{box-shadow:var(--shadow-lg)}
    .form-card.active::after{opacity:1}
    .form-card.cliente.active{border-color:rgba(59,125,216,0.3)}
    .form-card.proveedor.active{border-color:rgba(58,145,82,0.3)}
    .card-badge{display:inline-flex;align-items:center;gap:5px;border-radius:100px;padding:4px 11px;font-size:11px;font-weight:600;letter-spacing:0.3px;text-transform:uppercase;margin-bottom:16px}
    .badge-cli{background:var(--blue-light);color:var(--blue)}
    .badge-pro{background:var(--green-light);color:var(--green)}
    .card-icon{width:52px;height:52px;border-radius:var(--radius-md);display:flex;align-items:center;justify-content:center;font-size:24px;margin-bottom:16px}
    .icon-cli{background:var(--blue-light)}
    .icon-pro{background:var(--green-light)}
    .form-card h2{font-family:'DM Serif Display',serif;font-size:22px;font-weight:400;color:var(--gray-900);margin-bottom:8px}
    .form-card p{font-size:13px;color:var(--gray-500);line-height:1.6;margin-bottom:20px}
    .card-check{position:absolute;top:16px;right:16px;width:26px;height:26px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:12px;opacity:0;transition:opacity 0.2s,transform 0.2s;transform:scale(0.7)}
    .form-card.active .card-check{opacity:1;transform:scale(1)}
    .cliente .card-check{background:var(--blue);color:white}
    .proveedor .card-check{background:var(--green-mid);color:white}
    .btn-download{display:inline-flex;align-items:center;gap:8px;padding:9px 16px;border-radius:var(--radius-sm);font-size:13px;font-weight:500;cursor:pointer;text-decoration:none;transition:all 0.18s;border:1.5px solid}
    .btn-dl-cli{color:var(--blue);border-color:rgba(26,86,160,0.25);background:var(--blue-light)}
    .btn-dl-cli:hover{background:#d6e6f8;border-color:var(--blue)}
    .btn-dl-pro{color:var(--green);border-color:rgba(37,107,53,0.25);background:var(--green-light)}
    .btn-dl-pro:hover{background:#cce9d5;border-color:var(--green)}
    .divider{max-width:900px;margin:0 auto 40px;padding:0 2rem;display:flex;align-items:center;gap:16px}
    .divider-line{flex:1;height:1px;background:var(--gray-100)}
    .divider-label{font-size:12px;font-weight:500;color:var(--gray-300);text-transform:uppercase;letter-spacing:1px}
    .upload-wrapper{max-width:900px;margin:0 auto 80px;padding:0 2rem}
    .upload-box{background:var(--white);border:1.5px solid var(--gray-100);border-radius:var(--radius-lg);overflow:hidden;box-shadow:var(--shadow-sm)}
    .upload-header{padding:1.75rem 2rem 1.5rem;border-bottom:1px solid var(--gray-100);display:flex;align-items:center;gap:14px}
    .upload-header-icon{width:42px;height:42px;border-radius:var(--radius-sm);display:flex;align-items:center;justify-content:center;font-size:20px;flex-shrink:0;transition:background 0.3s}
    .icon-h-cli{background:var(--blue-light)}
    .icon-h-pro{background:var(--green-light)}
    .upload-header h3{font-family:'DM Serif Display',serif;font-size:20px;font-weight:400;color:var(--gray-900);margin-bottom:2px}
    .upload-header p{font-size:13px;color:var(--gray-500)}
    .upload-body{padding:2rem}
    .steps{display:flex;gap:0;margin-bottom:2rem;background:var(--gray-50);border-radius:var(--radius-md);padding:16px}
    .step-item{flex:1;display:flex;align-items:flex-start;gap:10px;position:relative}
    .step-item:not(:last-child)::after{content:'→';position:absolute;right:-4px;top:3px;color:var(--gray-300);font-size:12px}
    .step-num{width:24px;height:24px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:600;flex-shrink:0;transition:background 0.3s}
    .step-num-cli{background:var(--blue);color:white}
    .step-num-pro{background:var(--green-mid);color:white}
    .step-text{font-size:12px;color:var(--gray-500);line-height:1.4;padding-top:3px}
    .step-text strong{color:var(--gray-700);display:block;font-size:12px;font-weight:600}
    .fields-row{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}
    .field{display:flex;flex-direction:column;gap:6px}
    .field label{font-size:12px;font-weight:600;color:var(--gray-500);letter-spacing:0.2px}
    .field input{padding:10px 14px;border:1.5px solid var(--gray-100);border-radius:var(--radius-sm);font-family:'DM Sans',sans-serif;font-size:14px;color:var(--gray-900);background:var(--white);transition:border-color 0.2s,box-shadow 0.2s;outline:none}
    .field input:focus{border-color:var(--blue);box-shadow:0 0 0 3px rgba(59,125,216,0.12)}
    .field input.pro:focus{border-color:var(--green-mid);box-shadow:0 0 0 3px rgba(58,145,82,0.12)}
    .dropzone{border:2px dashed var(--gray-100);border-radius:var(--radius-md);padding:2.5rem 2rem;text-align:center;cursor:pointer;transition:border-color 0.2s,background 0.2s;background:var(--gray-50);margin-bottom:20px;position:relative}
    .dropzone:hover,.dropzone.dragover{background:var(--blue-light);border-color:var(--blue-mid)}
    .dropzone.pro:hover,.dropzone.pro.dragover{background:var(--green-light);border-color:var(--green-mid)}
    .dropzone input[type="file"]{position:absolute;inset:0;opacity:0;cursor:pointer;width:100%;height:100%}
    .dz-icon{font-size:32px;margin-bottom:10px}
    .dropzone h4{font-size:15px;font-weight:500;color:var(--gray-700);margin-bottom:4px}
    .dropzone p{font-size:13px;color:var(--gray-500)}
    .dz-types{display:inline-block;margin-top:10px;background:var(--white);border:1px solid var(--gray-100);border-radius:100px;padding:3px 12px;font-size:11px;font-weight:500;color:var(--gray-500)}
    .file-preview{display:none;background:var(--gray-50);border:1.5px solid var(--gray-100);border-radius:var(--radius-sm);padding:12px 16px;align-items:center;gap:12px;margin-bottom:20px}
    .file-preview.show{display:flex}
    .file-icon{width:38px;height:38px;background:#1D6F42;border-radius:6px;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;color:white;flex-shrink:0}
    .file-info{flex:1;min-width:0}
    .file-name{font-size:13px;font-weight:500;color:var(--gray-900);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .file-size{font-size:11px;color:var(--gray-500);margin-top:2px}
    .file-remove{width:28px;height:28px;border-radius:50%;border:none;background:var(--gray-100);color:var(--gray-500);cursor:pointer;font-size:14px;display:flex;align-items:center;justify-content:center;transition:background 0.15s}
    .file-remove:hover{background:#fce8e8;color:#c0392b}
    .btn-submit{width:100%;padding:14px;border:none;border-radius:var(--radius-sm);font-family:'DM Sans',sans-serif;font-size:15px;font-weight:600;color:white;cursor:pointer;transition:opacity 0.2s,transform 0.15s;display:flex;align-items:center;justify-content:center;gap:8px}
    .btn-submit:hover:not(:disabled){opacity:0.9}
    .btn-submit:active{transform:scale(0.99)}
    .btn-submit:disabled{opacity:0.6;cursor:not-allowed}
    .btn-submit-cli{background:var(--blue)}
    .btn-submit-pro{background:var(--green)}
    .success-toast{display:none;margin-top:16px;padding:14px 18px;border-radius:var(--radius-sm);font-size:14px;font-weight:500;align-items:center;gap:10px;animation:fadeIn 0.3s ease}
    .success-toast.show{display:flex}
    .success-toast.cli{background:var(--blue-light);color:var(--blue)}
    .success-toast.pro{background:var(--green-light);color:var(--green)}
    .error-toast{display:none;margin-top:16px;padding:14px 18px;border-radius:var(--radius-sm);font-size:14px;font-weight:500;align-items:center;gap:10px;background:#fce8e8;color:#a93226;animation:fadeIn 0.3s ease}
    .error-toast.show{display:flex}
    @keyframes fadeIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
    footer{border-top:1px solid var(--gray-100);padding:24px 2rem;text-align:center;font-size:12px;color:var(--gray-300)}
    @media(max-width:620px){.cards-section{grid-template-columns:1fr}.fields-row{grid-template-columns:1fr}.steps{flex-direction:column;gap:12px}.step-item::after{display:none}.upload-body{padding:1.25rem}.hero{padding:48px 1.25rem 40px}}
  </style>
</head>
<body>

<nav>
  <div class="nav-logo">Mi<span>Empresa</span></div>
  <a href="#formularios" class="nav-link">Formularios</a>
</nav>

<section class="hero">
  <div class="hero-eyebrow"><span class="dot"></span>Registro en línea</div>
  <h1>Únete como <em>cliente</em><br>o <em>proveedor</em></h1>
  <p>Descarga el formulario que corresponda, complétalo y envíanoslo. Te contactaremos a la brevedad.</p>
</section>

<section class="cards-section" id="formularios">
  <div class="form-card cliente active" onclick="selectTipo('cliente')">
    <div class="card-check">✓</div>
    <div class="card-badge badge-cli">Cliente</div>
    <div class="card-icon icon-cli">🏢</div>
    <h2>Soy cliente</h2>
    <p>Regístrate para acceder a nuestros productos y servicios con condiciones preferenciales.</p>
    <a class="btn-download btn-dl-cli" href="formulario_cliente.xlsx" download onclick="event.stopPropagation()">
      <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M7 1v8M4 6l3 3 3-3M2 11h10" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>
      Descargar formulario
    </a>
  </div>
  <div class="form-card proveedor" onclick="selectTipo('proveedor')">
    <div class="card-check">✓</div>
    <div class="card-badge badge-pro">Proveedor</div>
    <div class="card-icon icon-pro">🤝</div>
    <h2>Soy proveedor</h2>
    <p>Únete a nuestra red de proveedores y expande tu negocio trabajando con nosotros.</p>
    <a class="btn-download btn-dl-pro" href="formulario_proveedor.xlsx" download onclick="event.stopPropagation()">
      <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M7 1v8M4 6l3 3 3-3M2 11h10" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>
      Descargar formulario
    </a>
  </div>
</section>

<div class="divider">
  <div class="divider-line"></div>
  <span class="divider-label">Paso 2 — Envía tu formulario</span>
  <div class="divider-line"></div>
</div>

<div class="upload-wrapper">
  <div class="upload-box">
    <div class="upload-header">
      <div class="upload-header-icon icon-h-cli" id="hdrIcon">🏢</div>
      <div>
        <h3 id="hdrTitle">Envío de formulario — Cliente</h3>
        <p id="hdrDesc">Completa el Excel descargado y adjúntalo aquí para enviarlo.</p>
      </div>
    </div>
    <div class="upload-body">
      <div class="steps">
        <div class="step-item">
          <div class="step-num step-num-cli" id="s1">1</div>
          <div class="step-text"><strong>Descarga</strong>el formulario de arriba</div>
        </div>
        <div class="step-item">
          <div class="step-num step-num-cli" id="s2">2</div>
          <div class="step-text"><strong>Rellena</strong>el archivo en Excel</div>
        </div>
        <div class="step-item">
          <div class="step-num step-num-cli" id="s3">3</div>
          <div class="step-text"><strong>Adjunta</strong>y envía aquí</div>
        </div>
      </div>

      <form id="mainForm" action="https://formsubmit.co/santiago2424241@gmail.com" method="POST" enctype="multipart/form-data">
        <input type="hidden" name="_subject" id="emailSubject" value="Nuevo formulario de Cliente — Mi Empresa">
        <input type="hidden" name="_captcha" value="false">
        <input type="hidden" name="_template" value="table">
        <input type="hidden" name="_next" value="">
        <input type="hidden" name="tipo" id="campoTipo" value="Cliente">

        <div class="fields-row">
          <div class="field">
            <label>Nombre completo</label>
            <input type="text" name="nombre" id="fNombre" placeholder="Ej: Juan Pérez" required/>
          </div>
          <div class="field">
            <label>Correo electrónico</label>
            <input type="email" name="email" id="fEmail" placeholder="correo@empresa.com" required/>
          </div>
        </div>
        <div class="fields-row">
          <div class="field">
            <label>Empresa</label>
            <input type="text" name="empresa" id="fEmpresa" placeholder="Ej: Empresa S.A.S."/>
          </div>
          <div class="field">
            <label>Teléfono</label>
            <input type="tel" name="telefono" id="fTel" placeholder="+57 300 000 0000"/>
          </div>
        </div>

        <div class="dropzone" id="dropzone">
          <input type="file" name="attachment" id="fileInput" accept=".xlsx,.xls"/>
          <div class="dz-icon">📎</div>
          <h4>Arrastra tu formulario aquí</h4>
          <p>o haz clic para seleccionar el archivo</p>
          <span class="dz-types">.xlsx · .xls</span>
        </div>

        <div class="file-preview" id="filePreview">
          <div class="file-icon">XLS</div>
          <div class="file-info">
            <div class="file-name" id="fileName">—</div>
            <div class="file-size" id="fileSize">—</div>
          </div>
          <button type="button" class="file-remove" onclick="removeFile()">✕</button>
        </div>

        <button type="submit" class="btn-submit btn-submit-cli" id="btnSubmit" onclick="return validar()">
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M2 8l5 5 7-9" stroke="white" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>
          Enviar formulario
        </button>
      </form>

      <div class="success-toast cli" id="successMsg">
        <span>✅</span>
        <span id="successText">¡Formulario enviado! Te contactaremos pronto.</span>
      </div>
      <div class="error-toast" id="errorMsg">
        <span>⚠️</span>
        <span id="errorText">Ocurrió un error. Intenta de nuevo.</span>
      </div>
    </div>
  </div>
</div>

<footer>© 2025 MiEmpresa S.A.S. · Todos los derechos reservados</footer>

<script>
  let tipo = 'cliente';

  function selectTipo(t) {
    tipo = t;
    const isC = t === 'cliente';
    document.querySelector('.form-card.cliente').classList.toggle('active', isC);
    document.querySelector('.form-card.proveedor').classList.toggle('active', !isC);
    document.getElementById('hdrIcon').textContent = isC ? '🏢' : '🤝';
    document.getElementById('hdrIcon').className = 'upload-header-icon ' + (isC ? 'icon-h-cli' : 'icon-h-pro');
    document.getElementById('hdrTitle').textContent = isC ? 'Envío de formulario — Cliente' : 'Envío de formulario — Proveedor';
    document.getElementById('hdrDesc').textContent = isC ? 'Completa el Excel descargado y adjúntalo aquí para enviarlo.' : 'Completa el Excel de proveedor y adjúntalo aquí para enviarlo.';
    ['s1','s2','s3'].forEach(id => { document.getElementById(id).className = 'step-num ' + (isC ? 'step-num-cli' : 'step-num-pro'); });
    document.getElementById('dropzone').className = isC ? 'dropzone' : 'dropzone pro';
    document.querySelectorAll('.field input').forEach(inp => { if(inp.type !== 'hidden') inp.className = isC ? '' : 'pro'; });
    document.getElementById('btnSubmit').className = 'btn-submit ' + (isC ? 'btn-submit-cli' : 'btn-submit-pro');
    document.getElementById('successMsg').className = 'success-toast ' + (isC ? 'cli' : 'pro');
    document.getElementById('campoTipo').value = isC ? 'Cliente' : 'Proveedor';
    document.getElementById('emailSubject').value = 'Nuevo formulario de ' + (isC ? 'Cliente' : 'Proveedor') + ' — Mi Empresa';
    removeFile();
    document.getElementById('successMsg').classList.remove('show');
    document.getElementById('errorMsg').classList.remove('show');
  }

  const fileInput = document.getElementById('fileInput');
  const dropzone  = document.getElementById('dropzone');
  fileInput.addEventListener('change', () => { if (fileInput.files[0]) showFile(fileInput.files[0]); });
  dropzone.addEventListener('dragover',  e => { e.preventDefault(); dropzone.classList.add('dragover'); });
  dropzone.addEventListener('dragleave', () => dropzone.classList.remove('dragover'));
  dropzone.addEventListener('drop', e => {
    e.preventDefault(); dropzone.classList.remove('dragover');
    const f = e.dataTransfer.files[0];
    if (f) { fileInput.files = e.dataTransfer.files; showFile(f); }
  });
  function showFile(f) {
    document.getElementById('fileName').textContent = f.name;
    document.getElementById('fileSize').textContent = (f.size/1024).toFixed(1) + ' KB';
    document.getElementById('filePreview').classList.add('show');
    dropzone.style.display = 'none';
  }
  function removeFile() {
    fileInput.value = '';
    document.getElementById('filePreview').classList.remove('show');
    dropzone.style.display = '';
  }

  function validar() {
    const nombre  = document.getElementById('fNombre').value.trim();
    const email   = document.getElementById('fEmail').value.trim();
    const archivo = fileInput.files[0];
    document.getElementById('errorMsg').classList.remove('show');
    if (!nombre || !email) { mostrarError('Por favor completa el nombre y el correo antes de enviar.'); return false; }
    if (!archivo)          { mostrarError('Por favor adjunta el formulario Excel antes de enviar.'); return false; }
    return true;
  }

  function mostrarError(msg) {
    document.getElementById('errorText').textContent = msg;
    document.getElementById('errorMsg').classList.add('show');
  }
</script>
</body>
</html>