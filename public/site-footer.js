async function bootstrapSiteFooter() {
  const footer = document.querySelector("[data-site-footer]");
  if (!footer) {
    return;
  }

  const authorEl = footer.querySelector("[data-footer-author]");
  const versionEl = footer.querySelector("[data-footer-version]");
  const githubEl = footer.querySelector("[data-footer-github]");
  const githubTextEl = githubEl?.querySelector(".site-footer-sr-only");

  const fallbackMeta = {
    author: authorEl?.getAttribute("data-fallback") || "Kale",
    versionLabel: versionEl?.getAttribute("data-fallback") || "v0.1.22",
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
      authorEl.textContent = author;
    }
    if (versionEl) {
      versionEl.textContent = versionLabel;
    }
    if (githubEl) {
      githubEl.href = repositoryUrl;
      githubEl.setAttribute("aria-label", `GitHub：${repositoryLabel}`);
      githubEl.setAttribute("title", `GitHub：${repositoryLabel}`);
    }
    if (githubTextEl) {
      githubTextEl.textContent = `GitHub：${repositoryLabel}`;
    }
  } catch (_error) {
    if (authorEl) {
      authorEl.textContent = fallbackMeta.author;
    }
    if (versionEl) {
      versionEl.textContent = fallbackMeta.versionLabel;
    }
    if (githubEl) {
      githubEl.href = fallbackMeta.repositoryUrl;
      githubEl.setAttribute("aria-label", `GitHub：${fallbackMeta.repositoryLabel}`);
      githubEl.setAttribute("title", `GitHub：${fallbackMeta.repositoryLabel}`);
    }
    if (githubTextEl) {
      githubTextEl.textContent = `GitHub：${fallbackMeta.repositoryLabel}`;
    }
  }
}

bootstrapSiteFooter();
