import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import {
  DiagramSidePanelProvider,
  useDiagramSidePanel,
} from './diagram-side-panel-controller';
import { ResponsiveEditPopover } from './responsive-edit-popover';

function Harness() {
  const panel = useDiagramSidePanel();

  return (
    <>
      <ResponsiveEditPopover
        open
        anchorReference="anchorPosition"
        anchorPosition={{ left: 900, top: 300 }}
      >
        editor
      </ResponsiveEditPopover>
      <button
        type="button"
        onClick={() => panel.openEnvironments({ create: true })}
      >
        expand drawer
      </button>
      <button type="button" onClick={panel.close}>
        close drawer
      </button>
      <input aria-label="Environment Name" />
    </>
  );
}

describe('ResponsiveEditPopover', () => {
  beforeEach(() => {
    Object.defineProperty(window, 'innerWidth', {
      configurable: true,
      writable: true,
      value: 1440,
    });
  });

  test('preserves a left transition while moving with the drawer', async () => {
    render(
      <DiagramSidePanelProvider>
        <Harness />
      </DiagramSidePanelProvider>,
    );

    const paper = document.querySelector('.MuiPopover-paper') as HTMLElement;

    await waitFor(() => {
      expect(paper.style.left).toBe('524px');
      expect(getComputedStyle(paper).transition).toContain('left');
    });

    fireEvent.click(
      screen.getByRole('button', { name: 'expand drawer', hidden: true }),
    );

    await waitFor(() => {
      expect(paper.style.left).toBe('104px');
      expect(getComputedStyle(paper).transition).toContain('left');
    });

    fireEvent.click(
      screen.getByRole('button', { name: 'close drawer', hidden: true }),
    );

    await waitFor(() => {
      expect(paper.style.left).toBe('900px');
      expect(getComputedStyle(paper).transition).toContain('left');
    });
  });

  test('repositions when the viewport width changes', async () => {
    render(
      <DiagramSidePanelProvider>
        <Harness />
      </DiagramSidePanelProvider>,
    );

    const paper = document.querySelector('.MuiPopover-paper') as HTMLElement;
    window.innerWidth = 1200;
    fireEvent(window, new Event('resize'));

    await waitFor(() => {
      expect(paper.style.left).toBe('284px');
    });
  });

  test('allows environment fields outside the node editor to keep focus', async () => {
    render(
      <DiagramSidePanelProvider>
        <Harness />
      </DiagramSidePanelProvider>,
    );

    const environmentName = screen.getByRole('textbox', {
      name: 'Environment Name',
      hidden: true,
    });
    environmentName.focus();

    await waitFor(() => {
      expect(environmentName).toHaveFocus();
    });
  });
});
