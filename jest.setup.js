import '@testing-library/jest-dom'

// Polyfill ResizeObserver for recharts
global.ResizeObserver = class ResizeObserver {
  observe() {
    // do nothing
  }
  unobserve() {
    // do nothing
  }
  disconnect() {
    // do nothing
  }
};
