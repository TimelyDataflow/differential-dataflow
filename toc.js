// Populate the sidebar
//
// This is a script, and not included directly in the page, to control the total size of the book.
// The TOC contains an entry for each page, so if each page includes a copy of the TOC,
// the total size of the page becomes O(n**2).
class MDBookSidebarScrollbox extends HTMLElement {
    constructor() {
        super();
    }
    connectedCallback() {
        this.innerHTML = '<ol class="chapter"><li class="chapter-item expanded affix "><a href="introduction.html">Differential Dataflow</a></li><li class="chapter-item expanded "><a href="chapter_0/chapter_0.html"><strong aria-hidden="true">1.</strong> Motivation</a></li><li><ol class="section"><li class="chapter-item expanded "><a href="chapter_0/chapter_0_0.html"><strong aria-hidden="true">1.1.</strong> Getting started</a></li><li class="chapter-item expanded "><a href="chapter_0/chapter_0_1.html"><strong aria-hidden="true">1.2.</strong> Step 1: Write a program</a></li><li class="chapter-item expanded "><a href="chapter_0/chapter_0_2.html"><strong aria-hidden="true">1.3.</strong> Step 2: Change its input</a></li></ol></li><li class="chapter-item expanded "><a href="chapter_a/chapter_a.html"><strong aria-hidden="true">2.</strong> Increase all the things</a></li><li><ol class="section"><li class="chapter-item expanded "><a href="chapter_a/chapter_a_1.html"><strong aria-hidden="true">2.1.</strong> Increase the scale</a></li><li class="chapter-item expanded "><a href="chapter_a/chapter_a_2.html"><strong aria-hidden="true">2.2.</strong> Increase the parallelism</a></li><li class="chapter-item expanded "><a href="chapter_a/chapter_a_3.html"><strong aria-hidden="true">2.3.</strong> Increase the interactivity</a></li></ol></li><li class="chapter-item expanded "><a href="chapter_2/chapter_2.html"><strong aria-hidden="true">3.</strong> Differential Operators</a></li><li><ol class="section"><li class="chapter-item expanded "><a href="chapter_2/chapter_2_1.html"><strong aria-hidden="true">3.1.</strong> Map</a></li><li class="chapter-item expanded "><a href="chapter_2/chapter_2_2.html"><strong aria-hidden="true">3.2.</strong> Filter</a></li><li class="chapter-item expanded "><a href="chapter_2/chapter_2_3.html"><strong aria-hidden="true">3.3.</strong> Concat</a></li><li class="chapter-item expanded "><a href="chapter_2/chapter_2_4.html"><strong aria-hidden="true">3.4.</strong> Consolidate</a></li><li class="chapter-item expanded "><a href="chapter_2/chapter_2_5.html"><strong aria-hidden="true">3.5.</strong> Join</a></li><li class="chapter-item expanded "><a href="chapter_2/chapter_2_6.html"><strong aria-hidden="true">3.6.</strong> Reduce</a></li><li class="chapter-item expanded "><a href="chapter_2/chapter_2_7.html"><strong aria-hidden="true">3.7.</strong> Iterate</a></li><li class="chapter-item expanded "><a href="chapter_2/chapter_2_8.html"><strong aria-hidden="true">3.8.</strong> Arrange</a></li></ol></li><li class="chapter-item expanded "><a href="chapter_3/chapter_3.html"><strong aria-hidden="true">4.</strong> Differential Interactions</a></li><li><ol class="section"><li class="chapter-item expanded "><a href="chapter_3/chapter_3_1.html"><strong aria-hidden="true">4.1.</strong> Creating inputs</a></li><li class="chapter-item expanded "><a href="chapter_3/chapter_3_3.html"><strong aria-hidden="true">4.2.</strong> Making changes</a></li><li class="chapter-item expanded "><a href="chapter_3/chapter_3_4.html"><strong aria-hidden="true">4.3.</strong> Advancing time</a></li><li class="chapter-item expanded "><a href="chapter_3/chapter_3_2.html"><strong aria-hidden="true">4.4.</strong> Observing probes</a></li><li class="chapter-item expanded "><a href="chapter_3/chapter_3_5.html"><strong aria-hidden="true">4.5.</strong> Performing work</a></li></ol></li><li class="chapter-item expanded "><a href="chapter_4/chapter_4.html"><strong aria-hidden="true">5.</strong> Example Applications</a></li><li><ol class="section"><li class="chapter-item expanded "><a href="chapter_4/chapter_4_1.html"><strong aria-hidden="true">5.1.</strong> Graph Computation</a></li><li class="chapter-item expanded "><a href="chapter_4/chapter_4_2.html"><strong aria-hidden="true">5.2.</strong> Interactive Queries</a></li><li class="chapter-item expanded "><a href="chapter_4/chapter_4_3.html"><strong aria-hidden="true">5.3.</strong> Real-time Streaming Input</a></li></ol></li><li class="chapter-item expanded "><a href="chapter_5/chapter_5.html"><strong aria-hidden="true">6.</strong> Arrangements</a></li><li><ol class="section"><li class="chapter-item expanded "><a href="chapter_5/chapter_5_1.html"><strong aria-hidden="true">6.1.</strong> An arrangement example</a></li><li class="chapter-item expanded "><a href="chapter_5/chapter_5_2.html"><strong aria-hidden="true">6.2.</strong> Different arrangements</a></li><li class="chapter-item expanded "><a href="chapter_5/chapter_5_3.html"><strong aria-hidden="true">6.3.</strong> Sharing across dataflows</a></li><li class="chapter-item expanded "><a href="chapter_5/chapter_5_4.html"><strong aria-hidden="true">6.4.</strong> Trace wrappers</a></li></ol></li><li class="chapter-item expanded "><a href="chapter_6/chapter_6.html"><strong aria-hidden="true">7.</strong> Windows Enough and Time</a></li></ol>';
        // Set the current, active page, and reveal it if it's hidden
        let current_page = document.location.href.toString().split("#")[0];
        if (current_page.endsWith("/")) {
            current_page += "index.html";
        }
        var links = Array.prototype.slice.call(this.querySelectorAll("a"));
        var l = links.length;
        for (var i = 0; i < l; ++i) {
            var link = links[i];
            var href = link.getAttribute("href");
            if (href && !href.startsWith("#") && !/^(?:[a-z+]+:)?\/\//.test(href)) {
                link.href = path_to_root + href;
            }
            // The "index" page is supposed to alias the first chapter in the book.
            if (link.href === current_page || (i === 0 && path_to_root === "" && current_page.endsWith("/index.html"))) {
                link.classList.add("active");
                var parent = link.parentElement;
                if (parent && parent.classList.contains("chapter-item")) {
                    parent.classList.add("expanded");
                }
                while (parent) {
                    if (parent.tagName === "LI" && parent.previousElementSibling) {
                        if (parent.previousElementSibling.classList.contains("chapter-item")) {
                            parent.previousElementSibling.classList.add("expanded");
                        }
                    }
                    parent = parent.parentElement;
                }
            }
        }
        // Track and set sidebar scroll position
        this.addEventListener('click', function(e) {
            if (e.target.tagName === 'A') {
                sessionStorage.setItem('sidebar-scroll', this.scrollTop);
            }
        }, { passive: true });
        var sidebarScrollTop = sessionStorage.getItem('sidebar-scroll');
        sessionStorage.removeItem('sidebar-scroll');
        if (sidebarScrollTop) {
            // preserve sidebar scroll position when navigating via links within sidebar
            this.scrollTop = sidebarScrollTop;
        } else {
            // scroll sidebar to current active section when navigating via "next/previous chapter" buttons
            var activeSection = document.querySelector('#sidebar .active');
            if (activeSection) {
                activeSection.scrollIntoView({ block: 'center' });
            }
        }
        // Toggle buttons
        var sidebarAnchorToggles = document.querySelectorAll('#sidebar a.toggle');
        function toggleSection(ev) {
            ev.currentTarget.parentElement.classList.toggle('expanded');
        }
        Array.from(sidebarAnchorToggles).forEach(function (el) {
            el.addEventListener('click', toggleSection);
        });
    }
}
window.customElements.define("mdbook-sidebar-scrollbox", MDBookSidebarScrollbox);
