async function bootstrapSiteFooter() {
  const footer = document.querySelector("[data-site-footer]");
  if (!footer) {
    return;
  }

  const authorEl = footer.querySelector("[data-footer-author]");
  const versionEl = footer.querySelector("[data-footer-version]");
  const githubEl = footer.querySelector("[data-footer-github]");

  const fallbackMeta = {
    author: authorEl?.getAttribute("data-fallback") || "Kale",
    versionLabel: versionEl?.getAttribute("data-fallback") || "v0.1.8",
    repositoryUrl: githubEl?.getAttribute("href") || "https://github.com/icekale/stock-lu-tracker",
    repositoryLabel: githubEl?.getAttribute("data-fallback") || "icekale/stock-lu-tracker"
  };

  try {
    const response = await fetch("/api/app-meta", {
      headers: {
        "Content-Type": "application/json"
      }
    });
    if (!response.ok) {
      throw new Error("meta request failed");
    }

    const meta = await response.json();
    const author = String(meta.author || fallbackMeta.author).trim() || fallbackMeta.author;
    const versionLabel = String(meta.versionLabel || fallbackMeta.versionLabel).trim() || fallbackMeta.versionLabel;
    const repositoryUrl =
      String(meta.repositoryUrl || fallbackMeta.repositoryUrl).trim() || fallbackMeta.repositoryUrl;
    const repositoryLabel =
      String(meta.repositoryLabel || fallbackMeta.repositoryLabel).trim() || fallbackMeta.repositoryLabel;

    if (authorEl) {
      authorEl.textContent = `作者：${author}`;
    }
    if (versionEl) {
      versionEl.textContent = `版本：${versionLabel}`;
    }
    if (githubEl) {
      githubEl.href = repositoryUrl;
      githubEl.textContent = `GitHub：${repositoryLabel}`;
    }
  } catch (_error) {
    if (authorEl) {
      authorEl.textContent = `作者：${fallbackMeta.author}`;
    }
    if (versionEl) {
      versionEl.textContent = `版本：${fallbackMeta.versionLabel}`;
    }
    if (githubEl) {
      githubEl.href = fallbackMeta.repositoryUrl;
      githubEl.textContent = `GitHub：${fallbackMeta.repositoryLabel}`;
    }
  }
}

bootstrapSiteFooter();
