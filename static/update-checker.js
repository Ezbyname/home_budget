(function () {
    const GITHUB_REPO = 'ezbyname/home_budget';

    function compareVersions(a, b) {
        const pa = a.split('.').map(Number);
        const pb = b.split('.').map(Number);
        for (let i = 0; i < 3; i++) {
            if ((pa[i] || 0) > (pb[i] || 0)) return 1;
            if ((pa[i] || 0) < (pb[i] || 0)) return -1;
        }
        return 0;
    }

    function showUpdateBanner(version, downloadUrl) {
        const banner = document.createElement('div');
        banner.id = 'updateBanner';
        banner.style.cssText = [
            'position:fixed', 'top:0', 'left:0', 'right:0', 'z-index:10000',
            'background:#6366f1', 'color:#fff', 'padding:10px 20px',
            'display:flex', 'align-items:center', 'justify-content:center',
            'gap:12px', 'font-size:14px', 'box-shadow:0 2px 8px rgba(0,0,0,0.2)'
        ].join(';');
        banner.innerHTML = `
            <i class="bi bi-arrow-up-circle-fill" style="font-size:18px"></i>
            <span>גרסה <strong>${version}</strong> זמינה להורדה</span>
            <a href="${downloadUrl}" target="_blank"
               style="background:#fff;color:#6366f1;border-radius:6px;padding:4px 14px;font-weight:600;text-decoration:none;white-space:nowrap">
               הורד עדכון
            </a>
            <button onclick="document.getElementById('updateBanner').remove()"
                    style="background:none;border:none;color:#fff;font-size:22px;line-height:1;cursor:pointer;padding:0 4px"
                    title="סגור">×</button>
        `;
        document.body.prepend(banner);
    }

    fetch('/api/version')
        .then(r => r.json())
        .then(({ version }) => {
            fetch(`https://api.github.com/repos/${GITHUB_REPO}/releases/latest`, {
                headers: { 'User-Agent': 'home-budget-app' }
            })
                .then(r => r.json())
                .then(release => {
                    const latest = (release.tag_name || '').replace(/^v/, '');
                    if (latest && compareVersions(latest, version) > 0) {
                        const assets = release.assets || [];
                        const downloadUrl = assets.length
                            ? assets[0].browser_download_url
                            : release.html_url;
                        showUpdateBanner(latest, downloadUrl);
                    }
                })
                .catch(() => {});
        })
        .catch(() => {});
})();
