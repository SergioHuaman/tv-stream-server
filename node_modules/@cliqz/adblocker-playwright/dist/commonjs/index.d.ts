/*!
 * Copyright (c) 2017-present Ghostery GmbH. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
import * as pw from 'playwright';
import { FiltersEngine, Request } from '@cliqz/adblocker';
/**
 * Create an instance of `Request` from `pw.Request`.
 */
export declare function fromPlaywrightDetails(details: pw.Request): Request;
/**
 * Wrap `FiltersEngine` into a Playwright-friendly helper class.
 */
export declare class BlockingContext {
    private readonly page;
    private readonly blocker;
    private readonly onFrameNavigated;
    private readonly onRequest;
    constructor(page: pw.Page, blocker: PlaywrightBlocker);
    enable(): Promise<void>;
    disable(): Promise<void>;
}
/**
 * Wrap `FiltersEngine` into a Playwright-friendly helper class. It exposes
 * methods to interface with Playwright APIs needed to block ads.
 */
export declare class PlaywrightBlocker extends FiltersEngine {
    private readonly contexts;
    enableBlockingInPage(page: pw.Page): Promise<BlockingContext>;
    disableBlockingInPage(page: pw.Page): Promise<void>;
    isBlockingEnabled(page: pw.Page): boolean;
    onFrameNavigated: (frame: pw.Frame) => Promise<void>;
    private onFrame;
    onRequest: (route: pw.Route) => Promise<void>;
    private injectStylesIntoFrame;
    private injectScriptletsIntoFrame;
    /**
     * Look for sub-frames in `frame`, check if their `src` or `href` would be
     * blocked, and then proceed to removing them from the DOM completely.
     */
    private removeBlockedFrames;
}
export * from '@cliqz/adblocker';
//# sourceMappingURL=index.d.ts.map