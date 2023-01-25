const options = {
    moduleCache: {
        vue: Vue
    },
    async getFile(url) {
        const res = await fetch(url);
        if (!res.ok)
            throw Object.assign(new Error(res.statusText + ' ' + url), { res });
        return {
            getContentData: asBinary => asBinary ? res.arrayBuffer() : res.text(),
        }
    },
    addStyle(textContent) {
        const style = Object.assign(document.createElement('style'), { textContent });
        const ref = document.head.getElementsByTagName('style')[0] || null;
        document.head.insertBefore(style, ref);
    },
    compiledCache: {
    set(key, str) {     

    // naive storage space management
    for (;;) {

        try {

        // doc: https://developer.mozilla.org/en-US/docs/Web/API/Storage
        window.localStorage.setItem(key, str);
        break;
        } catch(ex) {

        // handle: Uncaught DOMException: Failed to execute 'setItem' on 'Storage': Setting the value of 'XXX' exceeded the quota

        window.localStorage.removeItem(window.localStorage.key(0));
        }
    }
    },
    get(key) {

    return window.localStorage.getItem(key);
    },
    },
}
const { loadModule } = window['vue3-sfc-loader'];
